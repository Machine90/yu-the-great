use super::{RaftTransporter, Transporter};
use crate::{rpc::RaftServiceClient};
use components::mailbox;
use mailbox::topo::Topo;
use crate::vendor::prelude::*;
use common::errors::Result as RaftResult;
use crate::{RaftMsg};
use common::protos::multi_proto::BatchMessages;
use common::{
    protocol::{GroupID, NodeID},
    protos::{raft_group_proto::GroupProto, raft_log_proto::Snapshot},
};
use components::mailbox::api::{GroupMailBox, NodeMailbox};
use tarpc_ext::tcp::rpc::tarpc::context;

impl From<(GroupID, RaftTransporter)> for GroupRpcClient {
    fn from(builder: (GroupID, RaftTransporter)) -> Self {
        Self {
            group: builder.0,
            transporter: builder.1,
        }
    }
}

#[derive(Clone)]
pub struct GroupRpcClient {
    group: GroupID,
    transporter: Transporter<RaftServiceClient>,
}

impl GroupRpcClient {
    pub fn from_transporter(group_id: GroupID, transporter: RaftTransporter) -> Self {
        Self {
            group: group_id,
            transporter,
        }
    }

    pub fn from_topo(group_id: GroupID, topo: Topo) -> Self {
        let transporter = RaftTransporter::from_topo(topo);
        Self::from_transporter(group_id, transporter)
    }

    pub fn fork(&self, new_group: GroupID) -> Self {
        assert!(
            new_group != self.group,
            "new group should be difference from current group"
        );
        Self {
            group: new_group,
            transporter: self.transporter.clone(),
        }
    }
}

impl GroupMailBox for GroupRpcClient {
    #[inline]
    fn get_group_id(&self) -> GroupID {
        self.group
    }
}

#[crate::async_trait]
impl NodeMailbox for GroupRpcClient {
    #[inline]
    fn topo(&self) -> Option<&Topo> {
        Some(&self.transporter.topo)
    }

    async fn sync_with(&self, node_id: NodeID, group_info: GroupProto) {
        let _ = self
            .transporter
            .make_call_to_node(node_id, move |service| {
                let group_info = group_info.clone();
                async move {
                    service
                        .assign_group(context::current(), node_id, group_info)
                        .await
                }
            })
            .await;
    }

    ///////////////////////////////////////////////////////
    ///      Interfaces for internal peers transfer msgs
    ///////////////////////////////////////////////////////

    async fn group_append(&self, group: GroupID, append: RaftMsg) -> RaftResult<RaftMsg> {
        let to = append.to;
        self.transporter
            .make_call_to_peer(group, to, move |service| {
                let append_msg = append.clone();
                async move { service.append(context::current(), group, append_msg).await }
            })
            .await?
            .into()
    }

    async fn group_append_response(
        &self,
        group: GroupID,
        append_response: RaftMsg,
    ) -> RaftResult<()> {
        let to = append_response.to;
        self.transporter
            .make_call_to_peer(group, to, move |service| {
                let append_msg = append_response.clone();
                async move {
                    service
                        .append_response(context::current(), group, append_msg)
                        .await
                }
            })
            .await?;
        Ok(())
    }

    async fn group_snapshot(&self, group: GroupID, snapshot: RaftMsg) -> RaftResult<RaftMsg> {
        let to = snapshot.to;
        let resp = self
            .transporter
            .make_call_to_peer(group, to, move |service| {
                let snapshot = snapshot.clone();
                async move {
                    service
                        .sync_snapshot(context::current(), group, snapshot)
                        .await
                }
            })
            .await?;
        resp.into()
    }

    async fn group_accepted_snapshot(
        &self,
        group: GroupID,
        reply_to: NodeID,
        snapshot: Snapshot,
    ) -> RaftResult<()> {
        let resp = self
            .transporter
            .make_call_to_peer(group, reply_to, move |service| {
                let snapshot = snapshot.clone();
                async move {
                    service
                        .accepted_snapshot(context::current(), group, reply_to, snapshot)
                        .await
                }
            })
            .await?;
        resp.into()
    }

    async fn group_report_snap_status(
        &self,
        group: GroupID,
        from: NodeID,
        leader_id: NodeID,
        status: String,
    ) -> RaftResult<()> {
        let resp = self
            .transporter
            .make_call_to_peer(group, leader_id, move |service| {
                let status = status.clone();
                async move {
                    service
                        .report_snapshot(context::current(), group, from, leader_id, status)
                        .await
                }
            })
            .await?;
        resp.into()
    }

    async fn group_request_vote(
        &self,
        group: GroupID,
        vote_request: RaftMsg,
    ) -> RaftResult<RaftMsg> {
        let to = vote_request.to;
        self.transporter
            .make_call_to_peer(group, to, move |service| {
                let vote = vote_request.clone();
                async move { service.request_vote(context::current(), group, vote).await }
            })
            .await?
            .into()
    }

    async fn group_send_heartbeat(
        &self,
        group: GroupID,
        heartbeat: RaftMsg,
    ) -> RaftResult<RaftMsg> {
        let to = heartbeat.to;
        self.transporter
            .make_call_to_peer(group, to, move |service| {
                let msg = heartbeat.clone();
                async move { service.heartbeat(context::current(), group, msg).await }
            })
            .await?
            .into()
    }

    async fn group_send_heartbeat_response(&self, group: GroupID, resp: RaftMsg) -> RaftResult<()> {
        let to = resp.to;
        self.transporter
            .make_call_to_peer(group, to, move |service| {
                let msg = resp.clone();
                async move {
                    service
                        .heartbeat_response(context::current(), group, msg)
                        .await
                }
            })
            .await?;
        Ok(())
    }

    async fn batch_heartbeats(
        &self,
        mut heartbeats: BatchMessages
    ) -> RaftResult<()> {
        let to = heartbeats.to;
        self.transporter
            .make_call_to_node(to, move |service| {
                let heartbeats = std::mem::take(&mut heartbeats);
                async move {
                    service.batch_heartbeats(context::current(), heartbeats).await
                }
            })
            .await?;
        Ok(())
    }

    async fn group_transfer_leader_timeout(
        &self,
        group: GroupID,
        timeout: RaftMsg,
    ) -> RaftResult<()> {
        let to = timeout.to;
        self.transporter
            .make_call_to_peer(group, to, move |service| {
                let msg = timeout.clone();
                async move {
                    service
                        .transfer_leader_timeout(context::current(), group, msg)
                        .await
                }
            })
            .await?
            .into()
    }

    async fn group_redirect_proposal(&self, group: GroupID, propose: RaftMsg) -> RaftResult<u64> {
        let to = propose.to;
        debug!("[RPC] group_redirect_proposal to {:?}", (group, to));
        self.transporter
            .make_call_to_peer(group, to, move |service| {
                let msg = propose.clone();
                async move {
                    debug!("[RPC] do call group_redirect_proposal to {:?}", (group, to));
                    service
                        .redirect_proposal(context::current(), group, msg)
                        .await
                }
            })
            .await?
            .into()
    }

    async fn group_redirect_transfer_leader(
        &self,
        group: GroupID,
        transfer: RaftMsg,
    ) -> RaftResult<()> {
        let to = transfer.to;
        self.transporter
            .make_call_to_peer(group, to, move |service| {
                let msg = transfer.clone();
                async move {
                    service
                        .redirect_transfer_leader(context::current(), group, msg)
                        .await
                }
            })
            .await?
            .into()
    }

    async fn group_redirect_read_index(
        &self,
        group: GroupID,
        readindex: RaftMsg,
    ) -> RaftResult<RaftMsg> {
        let to = readindex.to;
        self.transporter
            .make_call_to_peer(group, to, move |service| {
                let msg = readindex.clone();
                async move {
                    service
                        .redirect_read_index(context::current(), group, msg)
                        .await
                }
            })
            .await?
            .into()
    }
}
