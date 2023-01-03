use consensus::raft_node::raft_process::RaftProcess;
use consensus::raft_node::status::Status;

use crate::coprocessor::ChangeReason;
use crate::peer::process::at_most_one_msg;
use crate::{peer::Peer, RaftResult};
use crate::{
    ConsensusError, RaftMsg,
    RaftMsgType::{MsgRequestPreVote, MsgRequestVote},
};

impl Peer {
    /// Receive [MsgRequestVote](crate::RaftMsgType::MsgRequestVote)
    /// and [MsgRequestPreVote](crate::RaftMsgType::MsgRequestPreVote)
    /// from a remote (pre) candidate.
    pub async fn recv_vote(&self, vote: RaftMsg) -> RaftResult<RaftMsg> {
        let req_type = vote.msg_type();
        assert!(matches!(req_type, MsgRequestVote | MsgRequestPreVote));
        let mut raft = self.raft_group.wl_raft().await;
        let Status { soft_state, .. } = raft.status();
        let mut ready = raft.step_and_ready(vote)?;

        if let Some(ss) = ready.soft_state_ref() {
            self.on_soft_state_change(&soft_state, ss, ChangeReason::RecvVote).await;
        }

        self.persist_ready(&mut ready).await?;
        let applied_by_vote = self.apply_commit_entries(
            &mut raft, 
            ready.take_committed_entries()
        ).await;
        // a leader recv the vote, then just reject it.
        let reject_as_leader = at_most_one_msg(ready.take_messages());

        let mut light_ready = raft.advance_append(ready);
        let _ = self.persist_light_ready(&mut light_ready);
        self._advance_apply(raft, applied_by_vote).await;

        // recv vote as follower, maybe response if remote candidate's term >= my term
        let response_as_follower = at_most_one_msg(light_ready.take_messages());

        if let Some(resp) = reject_as_leader {
            return Ok(resp);
        }

        if let Some(resp) = response_as_follower {
            return Ok(resp);
        }
        // current follower does not attempt to response for a low-term candidate.
        Err(ConsensusError::Nothing)
    }
}
