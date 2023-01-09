use std::time::Duration;

use crate::vendor::prelude::*;
use consensus::{
    prelude::raft_role::RaftRole,
    raft_node::{raft_process::RaftProcess, status::Status},
};

use crate::coprocessor::ChangeReason;
use crate::{
    peer::pipeline::async_pipe::Pipelines,
    peer::{process::msgs, Core, Peer},
    ConsensusError, RaftMsg,
    RaftMsgType::*,
    RaftResult,
};

enum NextState {
    RequestVote(Vec<RaftMsg>),
    Split,
    Won(Vec<RaftMsg>),
    Pending,
    Lost,
}

impl NextState {
    /// If majority voters response, then we say it's reached quorum.
    /// When pre vote round enable, then the win of first round is to
    /// continue broadcast request vote.
    #[inline]
    pub fn reached_quorum(&self, is_first_round: bool) -> bool {
        match &self {
            NextState::Won(_) | NextState::Lost => true,
            NextState::RequestVote(_) => is_first_round,
            _ => false,
        }
    }
}

impl Peer {
    /// Broadcast request (pre) votes in the full process. If broadcast
    /// as pre_candidate, then broadcast prevote first and waiting for
    /// permits.
    /// ### Params
    /// * **as_role**: Only used for display.
    pub(crate) async fn broadcast_vote(
        &self,
        as_role: RaftRole,
        votes: Vec<RaftMsg>,
    ) -> RaftResult<RaftRole> {
        // // first round request (pre) vote
        debug!("bcast votes as {:?}", as_role);
        let next_state = self._broadcast_vote(true, votes).await?;
        let (finish, votes) = match next_state {
            // second round of pre-vote
            NextState::RequestVote(votes) => (false, Some(votes)),
            NextState::Won(append) => {
                // notify all follower that I became leader.
                let proposal = self
                    .broadcast_append(append, Duration::from_millis(1000))
                    .await?;
                debug!(
                    "won the election and notify voters with broadcast append: {:?}",
                    proposal
                );
                (true, None)
            }
            NextState::Lost => {
                debug!("lost the election, became follower again.");
                (true, None)
            }
            _ => (true, None),
        };

        if finish {
            return Ok(self.raft_group.role().await);
        }

        debug!("start second round election as Candidate");
        // second round request votes (if pre-vote enable)
        let votes = votes.unwrap();
        let next_state = self._broadcast_vote(false, votes).await?;
        match next_state {
            NextState::Won(append) => {
                // notify all follower that I became leader.
                let proposal = self
                    .broadcast_append(append, Duration::from_millis(1000))
                    .await?;
                debug!(
                    "won the election and notify voters with broadcast: {:?}",
                    proposal
                );
            }
            NextState::Lost => {
                debug!("lost the election, became follower again.");
            }
            _ => (),
        };
        Ok(self.raft_group.role().await)
    }

    /// broadcast votes internal
    async fn _broadcast_vote(
        &self,
        is_first_round: bool,
        votes: Vec<RaftMsg>,
    ) -> RaftResult<NextState> {
        let mut pipelines = Pipelines::new(self.mailbox.clone());
        let core = self.core.clone();
        pipelines.broadcast_with_async_cb(votes, move |voter, vote_resp| {
            let core = core.clone();
            async move { core._handle_vote_resp(voter, vote_resp).await }
        });
        let quorum_result = if is_first_round {
            pipelines
                .join_until(|resp| resp.is_ok() && resp.as_ref().unwrap().reached_quorum(true))
                .await
        } else {
            pipelines
                .join_until(|resp| resp.is_ok() && resp.as_ref().unwrap().reached_quorum(false))
                .await
        };
        if let Some((_, next)) = quorum_result {
            next
        } else {
            Err(ConsensusError::NotReachQuorum)
        }
    }
}

impl Core {
    async fn _handle_vote_resp(
        &self,
        voter: u64,
        vote_resp: RaftResult<RaftMsg>,
    ) -> RaftResult<NextState> {
        if let Err(e) = vote_resp {
            error!(
                "failed to collect vote result from {:?}, because: {:?}",
                voter, e
            );
            return match e {
                // remote peer with high-term didn't accepted the vote.
                ConsensusError::Nothing => Ok(NextState::Split),
                _ => return Err(e),
            };
        }
        let vote_resp = vote_resp.unwrap();
        let resp_type = vote_resp.msg_type();
        assert!(
            matches!(
                resp_type,
                MsgRequestPreVoteResponse | MsgRequestVoteResponse
            ),
            "unexpected response type of request (pre) vote"
        );

        let mut raft = self.raft_group.wl_raft().await;
        let Status { soft_state, .. } = raft.status();
        raft.step(vote_resp)?;

        if !raft.has_ready() {
            return Ok(NextState::Pending);
        }
        let mut ready = raft.get_ready();
        if let Some(ss) = ready.soft_state_ref() {
            self.on_soft_state_change(&soft_state, ss, ChangeReason::Election).await;
        }

        let role = raft.role();
        // maybe_commit_by_vote after step, and generate some committed entries.
        self.persist_ready(&mut ready).await?;
        // commit by vote, after received `vote_response` with larger commit at term.
        let (complete, applied_by_vote) = self.apply_commit_entries(
            &mut raft, 
            ready.take_committed_entries()
        ).await;
        self._advance_apply_to(&mut raft, applied_by_vote, complete).await;

        // if candidate become leader after won election
        let bcast_append = msgs(ready.take_messages());
        let mut light_ready = raft.advance_append(ready);
        drop(raft);
        
        let _ = self.persist_light_ready(&mut light_ready).await;
        // if pre-candidate become candidate after won first round election.
        let bcast_vote = msgs(light_ready.take_messages());

        // applied maybe changed to last committed entry.
        // release lock safety

        match resp_type {
            // request as pre-candidate, next request vote.
            MsgRequestPreVoteResponse => {
                // maybe pre-candidate => candidate and generate broadcast vote.
                if role == RaftRole::Follower {
                    // lost pre-election after tally votes, pre-candidate => follower
                    Ok(NextState::Lost)
                } else {
                    // otherwise: pre-candidate => candidate.
                    Ok(NextState::RequestVote(bcast_vote))
                }
            }
            MsgRequestVoteResponse => {
                // maybe became leader, and generate some campaign broadcast.
                if role == RaftRole::Follower {
                    // lost election after tally votes, candidate => follower
                    Ok(NextState::Lost)
                } else {
                    // otherwise: candidate => leader.
                    Ok(NextState::Won(bcast_append))
                }
            }
            _ => unreachable!(),
        }
    }
}
