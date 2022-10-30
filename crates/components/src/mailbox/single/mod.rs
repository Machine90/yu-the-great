use super::RaftEndpoint;
use crate::protos::{prelude::BatchConfChange, raft_payload_proto::StatusProto};
use common::protocol::PeerID;

use common::{errors::Result as RaftResult, protocol::read_state::ReadState};

/// RaftClient represent a client connect to a raft peer.
#[async_trait::async_trait]
pub trait RaftApi: Send + Sync {
    /// A raft client's id should be known.
    fn peer_id(&self) -> PeerID;

    /// get endpoint in RPC perform clone action for endpoint.
    fn get_endpoint(&self) -> std::io::Result<RaftEndpoint>;

    /// Send retrieve status request to target peer (e.g. Leader), then
    /// got status from that peer's Leader
    async fn status(&self) -> RaftResult<StatusProto>;

    /// Send propose to specific raft-peer with binary log content.
    /// If success, the latest commit id will return, otherwise
    /// return error with reason. This perform the `write` behavior of
    /// cluster.
    async fn propose(&self, log_content: Vec<u8>) -> RaftResult<u64>;

    async fn propose_conf_changes(&self, changes: BatchConfChange) -> RaftResult<StatusProto>;

    /// Send command to raft group in binary type, this command will be
    /// apply by [AdminLister](crate::coprocessor::listener::admin::AdminListener)
    /// `handle_cmds` method.
    async fn propose_cmd(&self, cmd: Vec<u8>) -> RaftResult<u64>;

    /// Read with index from current peer, then return (commit, readed_context)
    /// from group.
    async fn read_index(&self, index: Vec<u8>) -> RaftResult<ReadState>;

    /// Initiate an election for current peer.
    async fn election(&self) -> RaftResult<String>;
}
