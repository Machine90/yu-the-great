use crate::ConsensusError::*;
use consensus::raft_node::{raft_functions::RaftFunctions, raft_process::RaftProcess, Ready};
use crate::vendor::prelude::*;

use crate::{
    mailbox::api::MailBox,
    peer::{raft_group::raft_node::WLockRaft, Core, Peer},
    RaftMsg, RaftMsgType, RaftResult,
};

use super::{msgs, at_most_one_msg};

enum NextState {
    /// Leader decide to append lacked entries with transfee
    /// before transfer leadership to it.
    /// *RaftMsg*s in type **MsgAppend**.
    AppendToTransfee(Vec<RaftMsg>),
    /// Leader finish transfer with transfee and send
    /// MsgTimeoutNow to transfee to make it election.
    /// *RaftMsg* in type **MsgTimeoutNow**.
    EndWithTransfer(RaftMsg),

    /// Follower recv transfer request and send it to
    /// current Leader.
    /// *RaftMsg* in type **MsgTransferLeader**.
    ForawrdTransfer(RaftMsg),
}

impl Peer {
    /// Call transfer leadership as Leader or Follower, the leadership of
    /// current peer will be changed to specific transfee. It's difference from
    /// election, `transfer_leader` will sync raft log with transfee before
    /// transfer leadership to it if transfee is not replicated.
    /// A non-replicated follower will always lost election.
    pub async fn transfer_leader(&self, transfee: u64) -> RaftResult<()> {
        let mut raft = self.raft_group.wl_raft().await;
        raft.tranfer_leadership(transfee)?;
        let ready = raft.get_if_ready();
        // maybe follower or leader receive transfer request.
        self._advance_transfer_leader(transfee, raft, ready).await?;
        Ok(())
    }

    /// Leader receive forward transfer_leader msg from follower.
    pub async fn recv_leader_transfer(&self, transfer_msg: RaftMsg) -> RaftResult<()> {
        assert!(
            transfer_msg.msg_type() == RaftMsgType::MsgTransferLeader,
            "required for msg type in MsgTransferLeader"
        );
        let transfee = transfer_msg.from;
        let mut raft = self.wl_raft().await;
        let ready = raft.step_and_ready(transfer_msg);
        self._advance_transfer_leader(transfee, raft, ready).await?;
        Ok(())
    }

    /// Follower (transfee) received transfer leader response and start to
    /// campaign.
    pub async fn recv_transfer_timeout(&self, transfer_timeout: RaftMsg) -> RaftResult<()> {
        assert!(
            transfer_timeout.msg_type() == RaftMsgType::MsgTimeoutNow,
            "required for msg type in MsgTimeoutNow"
        );
        let mut raft = self.wl_raft().await;
        raft.step(transfer_timeout)?;

        if !raft.has_ready() {
            // when candidate receive MsgTimeoutNow, this msg will be ignored.
            return Ok(());
        }
        let ready = raft.get_ready();
        // unnecessary to log the changed role, the changes can be 
        // awared in `RaftListener`
        let _ = self._advance_hup_ready(raft, ready).await?;
        Ok(())
    }

    #[inline]
    async fn _advance_transfer_leader(
        &self,
        transfee: u64,
        raft: WLockRaft<'_>,
        ready: RaftResult<Ready>,
    ) -> RaftResult<()> {
        match self.core._next_transfer_step(raft, ready).await? {
            NextState::AppendToTransfee(appends) => {
                self.send_append_fut(Some(appends), "append to transfee");
            }
            NextState::EndWithTransfer(timeout_now) => {
                let _notified = self
                    .mailbox
                    .send_transfer_leader_timeout(timeout_now)
                    .await?;
            }
            NextState::ForawrdTransfer(forward_transfer) => {
                self.mailbox
                    .redirect_transfer_leader(forward_transfer)
                    .await?;
            }
        };
        Ok(())
    }
}

impl Core {
    async fn _next_transfer_step(
        &self,
        mut raft: WLockRaft<'_>,
        mut ready: RaftResult<Ready>,
    ) -> RaftResult<NextState> {
        let mut ready = match ready {
            Ok(ready) => ready,
            Err(Nothing) => {
                // maybe not leader now, or not progress of transfee, 
                // anyway, better to let caller know it.
                return Err(ProposalDropped(r#"failed to transfer leader, 
                maybe not leader now, or transfee is not a voter, more detail 
                please see the log"#.to_owned()));
            },
            Err(e) => {
                return Err(e);
            }
        };
        
        let mut msgs = msgs(ready.take_messages());

        let mut lrd = raft.advance(ready);
        let forward = at_most_one_msg(lrd.take_messages());

        let next = if !msgs.is_empty() {
            // leader
            match msgs[0].msg_type() {
                RaftMsgType::MsgAppend => {
                    NextState::AppendToTransfee(msgs)
                },
                RaftMsgType::MsgTimeoutNow => {
                    NextState::EndWithTransfer(msgs.pop().unwrap())
                }
                _ => unreachable!()
            }
        } else if forward.is_some() {
            NextState::ForawrdTransfer(forward.unwrap())
        } else {
            // TODO: replace "developer" to specific email.
            return Err(ProposalDropped("unexpected cause, please contact to developer".to_owned()));
        };
        Ok(next)
    }
}
