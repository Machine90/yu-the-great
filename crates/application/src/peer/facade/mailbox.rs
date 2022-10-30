use std::sync::Arc;

use crate::{
    peer::facade::local::LocalPeer, 
    protos::raft_payload_proto::Message as RaftMsg,
    ConsensusError
};
use common::{
    protocol::{GroupID, NodeID},
    protos::{multi_proto::BatchMessages, raft_group_proto::GroupProto}
};
use components::{mailbox::api::NodeMailbox};

use common::{errors::Result as RaftResult, protos::raft_log_proto::Snapshot};
use consensus::raft_node::SnapshotStatus;

/// Group manage trait, define all the operations for 
/// groups on the Node, a mailbox server should satisfied
/// this trait. 
pub trait GroupManage: Send + Sync {

    /// Get a group by id, a group on the node we 
    /// call it "peer".
    fn get_group(&self, group: GroupID) -> RaftResult<LocalPeer>;

    /// Maybe support create group on the node, for example
    /// multi-raft server.
    fn create_group(&self, group_info: GroupProto);
}

/// The mailbox service, performs as server of a "Node"
/// which implement `GroupManage` trait.
pub struct MailboxService  {
    pub manager: Arc<dyn GroupManage + 'static>
}

impl MailboxService {

    #[inline]
    fn get_group(&self, group: GroupID) -> RaftResult<LocalPeer> {
        self.manager.get_group(group)
    }
}

#[crate::async_trait]
impl NodeMailbox for MailboxService {

    async fn sync_with(&self, _: NodeID, group_info: GroupProto) {
        self.manager.create_group(group_info);
    }

    ///////////////////////////////////////////////////////
    ///      Interfaces for internal peers transfer msgs
    ///////////////////////////////////////////////////////
    async fn group_append(&self, group: GroupID, append: RaftMsg) -> RaftResult<RaftMsg> {
        self.get_group(group)?.recv_append(append).await
    }

    async fn group_append_response(
        &self,
        group: GroupID,
        append_response: RaftMsg,
    ) -> RaftResult<()> {
        self.get_group(group)?
            .handle_append_response(append_response)
            .await;
        Ok(())
    }

    async fn group_snapshot(&self, group: GroupID, snapshot: RaftMsg) -> RaftResult<RaftMsg> {
        self.get_group(group)?.recv_snapshot(snapshot).await
    }

    async fn group_accepted_snapshot(
        &self,
        group: GroupID,
        _: NodeID,
        snapshot: Snapshot,
    ) -> RaftResult<()> {
        self.get_group(group)?.recv_snapshot_reply(snapshot).await
    }

    async fn group_report_snap_status(
        &self,
        group: GroupID,
        from: NodeID,
        leader_id: NodeID,
        status: String,
    ) -> RaftResult<()> {
        let maybe_leader = self.get_group(group)?;
        let this_node = maybe_leader.node_id();
        if this_node != leader_id {
            let err_msg =
                format!("{this_node} is not leader, and reject to accept `report_snapshot`");
            return Err(ConsensusError::Other(err_msg.into()));
        }
        maybe_leader.recv_report_snap_status(
            from, 
            SnapshotStatus::from(status)
        ).await;
        Ok(())
    }

    async fn group_request_vote(
        &self,
        group: GroupID,
        vote_request: RaftMsg,
    ) -> RaftResult<RaftMsg> {
        self.get_group(group)?.recv_vote(vote_request).await
    }

    async fn group_send_heartbeat(
        &self,
        group: GroupID,
        heartbeat: RaftMsg,
    ) -> RaftResult<RaftMsg> {
        self.get_group(group)?.handle_heartbeat(heartbeat).await
    }

    async fn group_send_heartbeat_response(&self, group: GroupID, resp: RaftMsg) -> RaftResult<()> {
        self.get_group(group)?.handle_hb_ack(resp).await;
        Ok(())
    }

    async fn batch_heartbeats(&self, batched: BatchMessages) -> RaftResult<()> {
        let BatchMessages { messages, .. } = batched;
        for mut hb in messages {
            let heartbeat = hb.message.take();
            if heartbeat.is_none() {
                continue;
            }
            if let Ok(peer) = self.get_group(hb.group) {
                peer.recv_heartbeat(heartbeat.unwrap()).await;
            }
        }
        Ok(())
    }

    async fn group_transfer_leader_timeout(
        &self,
        group: GroupID,
        timeout: RaftMsg,
    ) -> RaftResult<()> {
        self.get_group(group)?.recv_transfer_timeout(timeout).await
    }

    async fn group_redirect_proposal(
        &self,
        group: GroupID,
        propose: RaftMsg,
    ) -> RaftResult<NodeID> {
        let proposal = self.get_group(group)?.recv_forward_propose(propose).await?;
        let proposal: RaftResult<u64> = proposal.into();
        proposal.into()
    }

    async fn group_redirect_transfer_leader(
        &self,
        group: GroupID,
        transfer: RaftMsg,
    ) -> RaftResult<()> {
        let peer = self.get_group(group)?;
        peer.recv_leader_transfer(transfer).await
    }

    async fn group_redirect_read_index(
        &self,
        group: GroupID,
        readindex: RaftMsg,
    ) -> RaftResult<RaftMsg> {
        self.get_group(group)?.recv_forward_read(readindex).await
    }
}
