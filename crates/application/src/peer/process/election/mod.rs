
use components::vendor::debug;
use consensus::prelude::*;

use crate::{
    peer::{raft_group::raft_node::WLockRaft, Peer},
    RaftResult, coprocessor::ChangeReason,
};

use super::msgs;
pub mod candidate;
pub mod follower;

#[allow(unused)]
impl Peer {

    /// Make an election for this raft peer. Maybe success and 
    /// became Leader after campaigned.
    pub async fn election(&self) -> RaftResult<RaftRole> {
        let mut raft = self.wl_raft().await;
        let role = raft.role();
        debug!("try to request for election as {:?}", role);
        raft.election()?;
        if !raft.has_ready() {
            // Leader publish an election or only one voter in the campaigned.
            return Ok(role);
        }
        let ready = raft.get_ready();
        self._advance_hup_ready(raft, ready).await
    }

    pub(super) async fn _advance_hup_ready(
        &self, 
        mut raft: WLockRaft<'_>, 
        mut ready: Ready
    ) -> RaftResult<RaftRole> {
        if let Some(ss) = ready.soft_state_ref() {
            // standalone node may become leader immediately
            self.on_soft_state_change(raft.prev_ss(), ss, ChangeReason::Election).await;
        }
        // only follower & candidate peer can generate some ready.
        let mut lrd = raft.advance(ready);
        let mut role = raft.role();
        if !lrd.get_messages().is_empty() {
            let votes = msgs(lrd.take_messages());
            drop(raft);
            role = self.broadcast_vote(role, votes).await?;
        }
        Ok(role)
    }
}
