use super::topo::Topo;
use crate::protos::raft_payload_proto::{Message as RaftMsg, MessageType as RaftMsgType};
use common::{protocol::{GroupID, NodeID}, protos::multi_proto::BatchMessages};
use crate::vendor::prelude::*;

use common::{
    errors::Result as RaftResult,
    protos::{raft_group_proto::GroupProto, raft_log_proto::Snapshot},
};

#[async_trait::async_trait]
pub trait NodeMailbox: Send + Sync {
    #[inline]
    fn topo(&self) -> Option<&Topo> {
        None
    }

    async fn sync_with(&self, node_id: NodeID, group_info: GroupProto);

    ///////////////////////////////////////////////////////
    ///      Interfaces for internal peers transfer msgs
    ///////////////////////////////////////////////////////

    async fn group_append(&self, group: GroupID, append: RaftMsg) -> RaftResult<RaftMsg>;

    async fn group_append_async(&self, group: GroupID, append: RaftMsg) -> RaftResult<()>;

    async fn group_append_response(
        &self,
        group: GroupID,
        append_response: RaftMsg,
    ) -> RaftResult<()>;

    /// Receive snapshot from leader as follower then handle it at server side,
    /// or send snapshot to follower from leader at client side.
    async fn group_snapshot(&self, group: GroupID, snapshot: RaftMsg) -> RaftResult<RaftMsg>;

    /// When follower received snapshot metadata and decide to accept it (maybe a large item),
    /// then follower maybe reply this snapshot message with a ack.
    async fn group_accepted_snapshot(
        &self,
        group: GroupID,
        reply_to: NodeID,
        snapshot: Snapshot,
    ) -> RaftResult<()>;

    async fn group_report_snap_status(
        &self,
        group: GroupID,
        from: NodeID,
        leader_id: NodeID,
        status: String,
    ) -> RaftResult<()>;

    async fn group_request_vote(
        &self,
        group: GroupID,
        vote_request: RaftMsg,
    ) -> RaftResult<RaftMsg>;

    async fn group_send_heartbeat(&self, group: GroupID, heartbeat: RaftMsg)
        -> RaftResult<RaftMsg>;

    async fn group_send_heartbeat_response(&self, group: GroupID, resp: RaftMsg) -> RaftResult<()>;

    async fn batch_heartbeats(&self, heatrbeats: BatchMessages) -> RaftResult<()>;

    async fn group_transfer_leader_timeout(
        &self,
        group: GroupID,
        timeout: RaftMsg,
    ) -> RaftResult<()>;

    async fn group_redirect_proposal(&self, group: GroupID, propose: RaftMsg)
        -> RaftResult<NodeID>;

    async fn group_redirect_transfer_leader(
        &self,
        group: GroupID,
        transfer: RaftMsg,
    ) -> RaftResult<()>;

    async fn group_redirect_read_index(
        &self,
        group: GroupID,
        readindex: RaftMsg,
    ) -> RaftResult<RaftMsg>;
}

/// The `GroupMailBox` means a mailbox's groupid must be known.
/// And all mails will be transfered in this group. It's provide
/// basic raft message api functions.
pub trait GroupMailBox: NodeMailbox {
    /// Get current group's ID.
    fn get_group_id(&self) -> GroupID;
}

/// Abstract Raft mailbox, the mailbox guarantee the basic message
/// transfer between peers of specific raft group.
#[async_trait::async_trait]
pub trait MailBox: Send + Sync {
    /// Send msg append from leader to target follower, then
    /// waiting for msg append response in this call.
    async fn send_append(&self, append: RaftMsg) -> RaftResult<RaftMsg>;

    /// Send msg append from leader to follower, unlike method `send_append`,
    /// this approach only send message to target without waiting for response,
    /// the follower would response this append in future.
    async fn send_append_async(&self, append: RaftMsg) -> RaftResult<()>;

    /// Send msg append response from follower to leader.
    async fn send_append_response(&self, append_response: RaftMsg) -> RaftResult<()>;

    /// keep sending append (and snapshot) messages to follower, and return the last
    /// success append response to caller. This method do not guarantee all appends
    /// will be success.
    async fn batch_appends(&self, appends: Vec<RaftMsg>) -> Option<RaftMsg>;

    /// Send msg snapshot from leader to target follower on demands.
    /// this method called when leader found some follower's progress state
    /// in `Snapshot`.
    async fn send_snapshot(&self, snapshot: RaftMsg) -> RaftResult<RaftMsg>;

    /// When after follower received msg snapshot from leader (via `send_snapshot`)
    /// this follower will reply snapshot with some context to leader,
    /// and then perfom `recv_snapshot_reply` on the leader side
    async fn accepted_snapshot(&self, reply_to: NodeID, snapshot: Snapshot) -> RaftResult<()>;

    async fn report_snap_status(
        &self,
        from: NodeID,
        to_leader: NodeID,
        status: String,
    ) -> RaftResult<()>;

    async fn send_request_vote(&self, vote_request: RaftMsg) -> RaftResult<RaftMsg>;

    async fn send_heartbeat(&self, heartbeat: RaftMsg) -> RaftResult<RaftMsg>;

    async fn send_heartbeat_response(&self, heartbeat_resp: RaftMsg) -> RaftResult<()>;

    async fn send_transfer_leader_timeout(&self, timeout: RaftMsg) -> RaftResult<()>;

    /// When a follower received a propose request, it'll forward
    /// this propose to leader by calling this method.
    async fn redirect_proposal(&self, propose: RaftMsg) -> RaftResult<u64>;

    async fn redirect_transfer_leader(&self, transfer: RaftMsg) -> RaftResult<()>;

    /// When a follower received a read_index request, it'll forward
    /// this read to leader by calling this method.
    async fn redirect_read_index(&self, readindex: RaftMsg) -> RaftResult<RaftMsg>;
}

/// The shorthand of the group mailbox, so that all functions can be called without
/// specific groupid.
#[async_trait::async_trait]
impl MailBox for dyn GroupMailBox {
    #[inline]
    async fn send_append(&self, append: RaftMsg) -> RaftResult<RaftMsg> {
        assert_eq!(append.msg_type(), RaftMsgType::MsgAppend);
        let append_resp = self.group_append(self.get_group_id(), append).await?;
        assert_eq!(append_resp.msg_type(), RaftMsgType::MsgAppendResponse);
        Ok(append_resp)
    }

    #[inline]
    async fn send_append_async(&self, append: RaftMsg) -> RaftResult<()> {
        assert_eq!(append.msg_type(), RaftMsgType::MsgAppend);
        self.group_append_async(self.get_group_id(), append).await?;
        Ok(())
    }

    async fn send_append_response(&self, append_response: RaftMsg) -> RaftResult<()> {
        assert_eq!(append_response.msg_type(), RaftMsgType::MsgAppendResponse);
        self.group_append_response(self.get_group_id(), append_response)
            .await?;
        Ok(())
    }

    #[inline]
    async fn batch_appends(&self, appends: Vec<RaftMsg>) -> Option<RaftMsg> {
        let mut last_append_resp = None;
        for append in appends {
            let mt = append.msg_type();
            // only accept append and snapshot kinds message.
            assert!(matches!(
                mt,
                RaftMsgType::MsgAppend | RaftMsgType::MsgSnapshot
            ));
            let (msg_type, index, term, to) =
                (append.msg_type(), append.index, append.term, append.to);
            let append_resp = match msg_type {
                RaftMsgType::MsgAppend => self.send_append(append).await,
                RaftMsgType::MsgSnapshot => self.send_snapshot(append).await,
                // MsgTimeoutNow only support for async mode, without blocking any response.
                _ => unreachable!(),
            };
            if let Err(e) = append_resp {
                warn!(
                    "failed to send {:?} msg in index-{:?} term-{:?} to-{:?}, reason: {:?}",
                    mt, index, term, to, e
                );
                break;
            }
            last_append_resp = Some(append_resp.unwrap());
        }
        last_append_resp
    }

    #[inline]
    async fn send_snapshot(&self, snapshot: RaftMsg) -> RaftResult<RaftMsg> {
        assert_eq!(snapshot.msg_type(), RaftMsgType::MsgSnapshot);
        self.group_snapshot(self.get_group_id(), snapshot).await
    }

    #[inline]
    async fn accepted_snapshot(&self, reply_to: NodeID, snapshot: Snapshot) -> RaftResult<()> {
        self.group_accepted_snapshot(self.get_group_id(), reply_to, snapshot)
            .await
    }

    #[inline]
    async fn report_snap_status(
        &self,
        from: NodeID,
        to_leader: NodeID,
        status: String,
    ) -> RaftResult<()> {
        self.group_report_snap_status(self.get_group_id(), from, to_leader, status)
            .await
    }

    #[inline]
    async fn send_request_vote(&self, vote_request: RaftMsg) -> RaftResult<RaftMsg> {
        assert!(matches!(
            vote_request.msg_type(),
            RaftMsgType::MsgRequestVote | RaftMsgType::MsgRequestPreVote
        ));
        self.group_request_vote(self.get_group_id(), vote_request)
            .await
    }

    #[inline]
    async fn send_heartbeat(&self, heartbeat: RaftMsg) -> RaftResult<RaftMsg> {
        assert_eq!(heartbeat.msg_type(), RaftMsgType::MsgHeartbeat);
        self.group_send_heartbeat(self.get_group_id(), heartbeat)
            .await
    }

    #[inline]
    async fn send_heartbeat_response(&self, resp: RaftMsg) -> RaftResult<()> {
        assert!(matches!(
            resp.msg_type(),
            RaftMsgType::MsgHeartbeatResponse | RaftMsgType::MsgAppendResponse
        ));
        self.group_send_heartbeat_response(self.get_group_id(), resp)
            .await
    }

    #[inline]
    async fn send_transfer_leader_timeout(&self, timeout: RaftMsg) -> RaftResult<()> {
        assert_eq!(timeout.msg_type(), RaftMsgType::MsgTimeoutNow);
        self.group_transfer_leader_timeout(self.get_group_id(), timeout)
            .await
    }

    #[inline]
    async fn redirect_proposal(&self, propose: RaftMsg) -> RaftResult<NodeID> {
        assert_eq!(propose.msg_type(), RaftMsgType::MsgPropose);
        self.group_redirect_proposal(self.get_group_id(), propose)
            .await
    }

    #[inline]
    async fn redirect_transfer_leader(&self, transfer: RaftMsg) -> RaftResult<()> {
        assert_eq!(transfer.msg_type(), RaftMsgType::MsgTransferLeader);
        self.group_redirect_transfer_leader(self.get_group_id(), transfer)
            .await
    }

    #[inline]
    async fn redirect_read_index(&self, readindex: RaftMsg) -> RaftResult<RaftMsg> {
        assert_eq!(readindex.msg_type(), RaftMsgType::MsgReadIndex);
        self.group_redirect_read_index(self.get_group_id(), readindex)
            .await
    }
}
