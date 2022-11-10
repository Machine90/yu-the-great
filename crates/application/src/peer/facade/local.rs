use std::{sync::Arc, time::Duration, ops::Deref};

use crate::{
    NodeID,
    Yusult,
    mailbox::{RaftEndpoint},
    peer::{
        facade::{AbstractPeer, Facade},
        process::{tick::Ticked},
        Peer, PeerID,
    },
    RaftMsg, RaftResult, coprocessor::read_index_ctx::{ReadContext, EvaluateRead}
};
use crate::async_trait;
use common::{protos::raft_log_proto::Snapshot, protocol::{read_state::ReadState, proposal::Proposal}};
use consensus::{prelude::{raft_role::RaftRole}, raft_node::SnapshotStatus};
use crate::protos::{prelude::BatchConfChange, raft_payload_proto::StatusProto};
use crate::vendor::prelude::*;

/// Reference to a raft peer.
/// ### Features (in plan)
/// * Handling basic request.
/// * Do rate limited
/// * Recording QPS
#[derive(Clone)]
pub struct LocalPeer {
    pub peer: Arc<Peer>,
}

impl LocalPeer {

    pub fn new(peer: Peer) -> Self {
        Self { peer: Arc::new(peer) }
    }

    #[inline]
    pub fn trace(&self, method: &str) {
        let (group, node) = self.my_id();
        trace!("[Local Peer] in group-{:?}, node-{:?} call method: {}", group, node, method);
    }

    #[inline]
    pub fn handle_timeout(&self) -> Duration {
        Duration::from_millis(1000)
    }

    /// Both incoming and outgoing voters of the peer.
    #[inline]
    pub async fn voters(&self) -> std::collections::HashSet<u64> {
        self.rl_raft().await.status().all_voters().unwrap_or_default()
    }

    #[inline]
    async fn _before_read(&self, read_request_ctx: &mut ReadContext) -> RaftResult<EvaluateRead> {
        self.coprocessor_driver
            .before_read_index(read_request_ctx)
            .await?;
        Ok(read_request_ctx.evaluate())
    }
}

impl Deref for LocalPeer {
    type Target = Peer;

    fn deref(&self) -> &Self::Target {
        self.peer.as_ref()
    }
}

#[async_trait]
impl AbstractPeer for LocalPeer {

    #[inline]
    fn my_id(&self) -> PeerID {
        self.peer.get_id()
    }

    #[inline]
    fn get_endpoint(&self) -> std::io::Result<RaftEndpoint> {
        Ok(self.peer.endpoint().clone())
    }

    async fn propose_async(&self, data: Vec<u8>) -> Yusult<Proposal> {
        self.trace("propose_async");
        let proposal = self
            .peer
            .handle_or_forward_propose(data, self.handle_timeout())
            .await?;
        Ok(proposal)
    }

    #[inline]
    async fn read_async(&self, index: Vec<u8>) -> Yusult<ReadState> {
        Ok(self.recv_read(index).await?)
    }

    async fn propose_conf_changes_async(
        &self, 
        changes: BatchConfChange,
        get_status: bool
    ) -> Yusult<Option<StatusProto>> {
        self.trace("propose_conf_changes_async");
        let _ = self
            .peer
            .handle_or_forward_conf_changes(changes, self.handle_timeout())
            .await?;
        if get_status {
            Ok(self.status_async().await.ok())
        } else {
            Ok(None)
        }
    }

    async fn propose_cmd_async(&self, cmd: Vec<u8>) -> Yusult<Proposal> {
        self.trace("propose_cmd_async");
        let proposal = self
            .peer
            .handle_or_forward_propose_cmd(cmd, self.handle_timeout())
            .await?;
        Ok(proposal)
    }

    #[inline]
    async fn status_async(&self) -> Yusult<StatusProto> {
        self.trace("status_async");
        Ok(self.peer.status(true).await)
    }

    #[inline] async fn tick_async(&self) -> Yusult<Ticked> {
        self.trace("tick_async");
        Ok(self.peer.tick(true, true).await?)
    }

    #[inline] async fn election_async(&self) -> Yusult<RaftRole> {
        self.trace("election");
        Ok(self.peer.election().await?)
    }

    #[inline] async fn transfer_leader_async(&self, to: NodeID) -> Yusult<()> {
        self.trace("transfer_leader");
        self.peer.transfer_leader(to).await?;
        Ok(())
    }
}

impl LocalPeer {

    pub async fn recv_propose(&self, log_content: Vec<u8>) -> RaftResult<Proposal> {
        self.trace("recv_propose");
        self.peer.handle_or_forward_propose(
            log_content, 
            self.handle_timeout() // TODO: replace with configgured value instead.
        ).await
    }

    /// Receive conchange type propose from external and
    /// handle it at local.
    pub async fn recv_conf_changes(&self, changes: BatchConfChange) -> RaftResult<Proposal> {
        self.trace("recv_conf_changes");
        self.peer.handle_or_forward_conf_changes(
            changes, 
            self.handle_timeout() // TODO: replace with configgured value instead.
        ).await
    }

    /// Receive command type propose from external and
    /// handle it at local.
    pub async fn recv_propose_cmd(&self, cmd: Vec<u8>) -> RaftResult<Proposal> {
        self.trace("recv_propose_cmd");
        self.peer.handle_or_forward_propose_cmd(
            cmd, 
            self.handle_timeout() // TODO: replace with configgured value instead.
        ).await
    }

    /// Receive forward propose as Leader.
    #[inline]
    pub async fn recv_forward_propose(&self, propose: RaftMsg) -> RaftResult<Proposal> {
        self.trace("recv_forward_propose");
        self.peer.recv_forward_propose(propose).await
    }

    /// Receive vote as one of voter in quorum.
    #[inline] 
    pub async fn recv_vote(&self, vote: RaftMsg) -> RaftResult<RaftMsg> {
        self.trace("recv_vote");
        self.peer.recv_vote(vote).await
    }

    pub async fn recv_read(&self, read_ctx: Vec<u8>) -> RaftResult<ReadState> {
        self.trace("recv_read");

        let mut read_ctx = ReadContext::from_read(read_ctx);

        match self._before_read(&mut read_ctx).await? {
            EvaluateRead::HasReaded(rs) => {
                rs.into()
            },
            EvaluateRead::NotExist => {
                // not found, then return empty.
                Ok(ReadState::default())
            },
            EvaluateRead::ShouldRead(pending) => {
                let peer = self.peer.clone();
                let resp = pending.before(async move {
                    let _ = peer.handle_or_forward_read(read_ctx).await;
                }, self.handle_timeout()).await?;
                resp.into()
            },
            EvaluateRead::MaybeExist => {
                // read directly.
                let rs = self
                    .peer
                    .handle_or_forward_read(read_ctx).await?;
                Ok(rs)
            }
            _ => {
                // this must be handle by developer who implement the coprocessor
                panic!("shouldn't evaluate to `ShouldResponse` when `recv_read`, 
                please check the method `before_read_index` in coprocessor impl");
            }
        }
    }

    /// Receive forward read from followers as Leader
    pub async fn recv_forward_read(&self, read_index: RaftMsg) -> RaftResult<RaftMsg> {
        self.trace("recv_forward_read");
        let mut read_ctx = ReadContext::from_forward(&read_index);

        match self._before_read(&mut read_ctx).await? {
            EvaluateRead::MaybeExist => {
                // read directly.
                let rs = self
                    .peer
                    .recv_forward_read(read_index, read_ctx).await?;
                Ok(rs)
            },
            EvaluateRead::HasForward(resp) => {
                resp.into()
            },
            EvaluateRead::ShouldForward(pending) => {
                // println!("ShouldForward...");
                let peer = self.peer.clone();
                let resp = pending.before(async move {
                    let _ = peer.recv_forward_read(read_index, read_ctx).await;
                }, self.handle_timeout()).await?;
                resp.into()
            },
            case => {
                panic!("shouldn't evaluate to {:?} when `recv_forward_read`, 
                please check the method `before_read_index` in coprocessor impl", case);
            }
        }
    }

    /// Receive append from leader as follower
    #[inline] 
    pub async fn recv_append(&self, append: RaftMsg) -> RaftResult<RaftMsg> {
        self.trace("recv_append");
        self.peer.recv_append(append).await
    }

    /// Receive snapshot from leader as follower.
    #[inline] 
    pub async fn recv_snapshot(&self, snap: RaftMsg) -> RaftResult<RaftMsg> {
        self.trace("recv_snapshot");
        self.peer.maybe_apply_snapshot(snap).await
    }

    #[inline]
    pub async fn recv_snapshot_reply(&self, snap: Snapshot) -> RaftResult<()> {
        self.peer.handle_snapshot_reply(snap).await
    }

    #[inline] 
    pub async fn recv_report_snap_status(&self, reporter: NodeID, status: SnapshotStatus) {
        self.peer.recv_report_snapshot(reporter, status).await;
    }

    #[inline] 
    pub async fn recv_transfer_timeout(&self, transfer_timeout: RaftMsg) -> RaftResult<()> {
        self.trace("recv_transfer_timeout");
        self.peer.recv_transfer_timeout(transfer_timeout).await
    }
}

impl Facade for LocalPeer {}

impl From<Peer> for LocalPeer {
    fn from(node: Peer) -> Self {
        Self {
            peer: Arc::new(node)
        }
    }
}
