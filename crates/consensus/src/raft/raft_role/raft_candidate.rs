use crate::{raft::{Raft}, storage::Storage};
use crate::protos::raft_payload_proto::{MessageType::*, Message};
use crate::{debug};
use crate::errors::*;

use super::{RaftRole, raft_follower::{FollowerHandler}};

pub trait CandidateRaft {
    fn process(&mut self, message: Message) -> Result<()>;
}

impl<STORAGE: Storage> CandidateRaft for Raft<STORAGE> {
    fn process(&mut self, message: Message) -> Result<()> {
        let (msg_from, msg_term) = (message.from, message.term);
        match message.msg_type() {
            MsgPropose => {
                let reason = format!("no leader at term {:?}", self.term);
                debug!("{:?}, decide to dropping proposal", reason);
                return Err(Error::ProposalDropped(reason));
            },
            // receive and handle `MsgAppend` from leader peer
            // then ack `MsgAppendResponse` to leader
            MsgAppend => {
                debug_assert_eq!(self.term, msg_term);
                self.become_follower(msg_term, msg_from);
                self.handle_append_entries(&message);
            },
            MsgHeartbeat => {
                debug_assert_eq!(self.term, msg_term);
                self.become_follower(msg_term, msg_from);
                self.handle_heartbeat(message);
            },
            MsgSnapshot => {
                debug_assert_eq!(self.term, msg_term);
                self.become_follower(msg_term, msg_from);
                self.handle_snapshot(message);
            },
            MsgRequestPreVoteResponse | MsgRequestVoteResponse => {
                let msg_type = message.msg_type();
                if self.current_raft_role == RaftRole::PreCandidate && msg_type != MsgRequestPreVoteResponse {
                    return Ok(());
                }
                if self.current_raft_role == RaftRole::Candidate && msg_type != MsgRequestVoteResponse {
                    return Ok(());
                }
                // record vote from other peer, and tally result.
                self.poll_and_handle_votes(msg_from, msg_type, !message.reject);
                self.maybe_commit_by_vote(&message);
            },
            MsgTimeoutNow => {
                debug!(
                    "{term} ignored MsgTimeoutNow from {from}",
                    term = self.term,
                    from = msg_from;
                    "state" => ?self.current_raft_role,
                );
            },
            _ => {}
        }
        Ok(())
    }
}
