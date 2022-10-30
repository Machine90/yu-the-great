use std::fmt::Display;

use crate::storage::Storage;

use super::{CAMPAIGN_TRANSFER, DUMMY_ID, DUMMY_TERM, Raft};
use crate::raft::raft_role::RaftRole as role;
use crate::protos::raft_payload_proto::{self as payload, MessageType::*};
use slog::Value;
use crate::debug;

#[derive(Debug)]
pub enum RaftCases {
    RequestElection,
    /// maybe step in case when recv `MsgRequestVote`<br/>
    /// and response `MsgRequestPreVoteResponse` in accept
    ApproveRequestVote,
    ApproveRequestPreVote,
    /// maybe step in case when recv `MsgRequestPreVote` or `MsgRequestVote`<br/>
    /// and response `MsgRequestPreVoteResponse` or `MsgRequestVoteResponse` in reject
    RejectRequestVote,
    /// In this case, no messages will be generated after step in. <br/>
    /// `MsgRequestVote`, `MsgRequestPreVote` could step in this case.
    HighTermPeerVoteWhenInLease,
    /// when received message with lower term from other raft nodes <br/>
    /// all these kinds messages will be ignored, and no messages response
    RecvLowerTerm,
    /// maybe occurred recv msg `MsgRequestPreVote` from `pre-candidate` <br/>
    /// and... then reject this `pre-candidate` in `MsgRequestPreVoteResponse`
    LowerTermCandidatePreVote,
    /// maybe occurred when recv msg `MsgHeartbeat` | `MsgAppend` from `"Leader"` <br/>
    /// and `"Leader"`'s Term is smaller than my Term, and also required `check_quorum` or `enable_pre_vote` <br/>
    /// then response `MsgAppendResponse` with my `Term` in accept response
    SplitBrain,

    // dispatch the normal message to each kinds of roles
    LeaderRecvMsg,
    FollowerRecvMsg,
    CandidateRecvMsg,
}

impl RaftCases {

    /// Process received message (both local and remote) with local raft before really consume it in the right way.
    /// There has 3 kinds cases when pre-handle message:
    /// * **Local Msg**: consume it directly when receive msg's term in 0, such like:
    ///     * [MsgHup](protos::raft_payload_proto::MessageType::MsgHup)
    ///     * [MsgBeat](protos::raft_payload_proto::MessageType::MsgBeat)
    ///     * [MsgUnreachable](protos::raft_payload_proto::MessageType::MsgUnreachable)
    ///     * [MsgCheckQuorum](protos::raft_payload_proto::MessageType::MsgCheckQuorum)
    ///     * [MsgSnapStatus](protos::raft_payload_proto::MessageType::MsgSnapStatus)
    /// * **Low Term Msg (remote)**:
    ///     When receive a remote message with low term, sometimes means remote peer has been split, then if:
    ///     * [MsgRequestPreVote](protos::raft_payload_proto::MessageType::MsgRequestPreVote): reject it's request.
    ///     * MsgHeartbeat or MsgAppend: if enable pre-vote or check-quorum, response it with my term in 
    ///       [MsgAppendResponse](protos::raft_payload_proto::MessageType::MsgAppendResponse)
    ///     * Otherwise: ignore this message.
    /// * **High Term Msg (remote)**:
    ///     Consume it after pre-handle. When receive this kind of message, always means that:
    ///     * MsgHeartbeat or MsgAppend or MsgSnapshot: Local peer maybe split, then become follower (of remote peer if it's leader). 
    ///     then handle this message as new role.
    ///     * MsgRequestPreVote or MsgRequestVote (and response): Just receive the vote request.
    /// * **Same Term**: 
    ///     Consume it directly
    pub fn receive<S: Storage>(local_raft: &mut Raft<S>, message: &payload::Message) -> Self {
        let (req_vote, req_pre_vote) = (
            message.msg_type() == MsgRequestVote,
            message.msg_type() == MsgRequestPreVote,
        );

        // case when receive a local message with term 0
        if message.term == DUMMY_TERM {
            return RaftCases::dispatch_message(local_raft, message);
        }
        // case when receive the hight term message from other peers
        else if message.term > local_raft.term {
            if req_vote || req_pre_vote {
                // if a server receives RequestVote request within the minimum election
                // timeout of hearing from a current leader, it does not update its term
                // or grant its vote
                //
                // This is included in the 3rd concern for Joint Consensus, where if another
                // peer is removed from the cluster it may try to hold elections and disrupt
                // stability.
                let force = message.context == CAMPAIGN_TRANSFER;
                let in_lease = Self::in_lease(local_raft);

                if !force && in_lease {
                    return Self::HighTermPeerVoteWhenInLease;
                }
            }

            let accept_pre_vote =
                message.msg_type() == MsgRequestPreVoteResponse && !message.reject;
            if req_pre_vote || accept_pre_vote {
                // just ignore when receive the accepted response of pre-vote request
            } else {
                // while receiving high term raft node's message, then become the follower 
                // (if message from Leader, then become it's follower)
                debug!("received a message with higher term from {from}",
                    from = message.from;
                    "term" => local_raft.term,
                    "message_term" => message.term,
                    "msg type" => ?message.msg_type());
                let leader_id = match message.msg_type() {
                    MsgAppend | MsgHeartbeat | MsgSnapshot => message.from,
                    _ => DUMMY_ID,
                };
                // [Tips] If role transfered to follower after received `MsgRequestVote` from 
                // the same voter in hight frequency, maybe there exists split peer, setting 
                // `enable_pre_vote_round` to true.
                local_raft.become_follower(message.term, leader_id);
            }
            // then continue to handle the message after peer's changed
            return RaftCases::dispatch_message(local_raft, message);
        }
        // case when receive a lower term from other peer's message
        else if message.term < local_raft.term {
            // when received message from leader and that message term lower than current raft node...
            if (local_raft.check_quorum || local_raft.enable_pre_vote)
                && match message.msg_type() {
                    MsgHeartbeat | MsgAppend => true,
                    _ => false,
                }
            {
                return Self::SplitBrain;
            } else if req_pre_vote {
                return Self::LowerTermCandidatePreVote;
            }
            return Self::RecvLowerTerm;
        }
        // otherwise the message received from other normal peers (in the same term), just step it.
        else {
            return RaftCases::dispatch_message(local_raft, message);
        }
    }

    fn dispatch_message<S: Storage>(peer: &mut Raft<S>, message: &payload::Message) -> Self {
        return match message.msg_type() {
            payload::MessageType::MsgHup => Self::RequestElection,
            // when current raft peer recev a vote request
            payload::MessageType::MsgRequestVote | payload::MessageType::MsgRequestPreVote => {
                let can_vote = Self::can_vote(peer, message);
                let (remote_idx, remote_log_term, remote_priority) = (message.index, message.log_term, message.priority);
                if can_vote 
                    && peer.raft_log.is_up_to_date(remote_idx, remote_log_term) 
                    && (peer.raft_log.last_index().unwrap() < remote_idx || peer.priority <= remote_priority)  
                {
                    if message.msg_type() == MsgRequestPreVote {
                        RaftCases::ApproveRequestPreVote
                    } else {
                        RaftCases::ApproveRequestVote
                    }
                } else {
                    RaftCases::RejectRequestVote
                }
            },
            // otherwise we step in raft peer in each state
            _ => match peer.current_raft_role {
                role::Leader => Self::LeaderRecvMsg,
                role::PreCandidate | role::Candidate => Self::CandidateRecvMsg,
                role::Follower => Self::FollowerRecvMsg,
            },
        };
    }

    #[inline]
    pub fn in_lease<S: Storage>(peer: &Raft<S>) -> bool {
        peer.check_quorum
            && peer.leader_id != DUMMY_ID
            && peer.election_elapsed < peer.election_timeout
    }

    /// Conditions of `can_vote`:
    /// * vote is not repeat received from same candidate.
    /// * has not vote for candidate recently and not leader now.
    /// * receive high term from pre-candidate
    #[inline]
    pub fn can_vote<S: Storage>(peer: &Raft<S>, request: &payload::Message) -> bool {
        // We can vote if this is a repeat of a vote we've already cast...
        let repeat_vote = peer.vote == request.from;
        // ...we haven't voted and we don't think there's a leader yet in this term...
        let has_not_leader_and_vote = peer.leader_id == DUMMY_ID && peer.vote == DUMMY_ID;
        // ...this is a PreVote for a future term...
        let request_pre_vote = request.msg_type() == MsgRequestPreVote && request.term > peer.term;
        repeat_vote || has_not_leader_and_vote || request_pre_vote
    }

    fn describe_case(&self) -> &'static str {
        match self {
            RaftCases::HighTermPeerVoteWhenInLease => "receive a (pre)vote msg with high term from remote when current peer's lease not expired",
            RaftCases::CandidateRecvMsg => "receive a msg as candidate",
            RaftCases::LowerTermCandidatePreVote => "receive a pre vote msg with low term from a candidate",
            RaftCases::RecvLowerTerm => "receive a message with low term",
            RaftCases::RequestElection => "some peer want to campain",
            RaftCases::ApproveRequestPreVote => "decide to approve the pre-vote",
            RaftCases::ApproveRequestVote => "decide to approve the vote",
            RaftCases::RejectRequestVote => "decide to reject the (pre)vote",
            RaftCases::SplitBrain => "receive a low term message from an expired leader",
            RaftCases::LeaderRecvMsg => "receive a msg as leader",
            RaftCases::FollowerRecvMsg => "receive a msg as follower",
        }
    }
}

impl Value for RaftCases {
    fn serialize(
        &self,
        _record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let case = self.describe_case();
        serializer.emit_str(key, case)
    }
}

impl Display for RaftCases {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let c = self.describe_case();
        write!(f, "{}", c)
    }
}
