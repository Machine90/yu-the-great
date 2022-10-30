pub mod client;
pub mod provider;
pub mod server;

use crate::RaftMsg;
use common::protocol::{response::Response, GroupID, NodeID};

use common::protos::multi_proto::BatchMessages;
use common::protos::{raft_group_proto::GroupProto, raft_log_proto::Snapshot};
pub use tarpc_ext;
use tarpc_ext::tcp::rpc::tarpc;

#[tarpc::service]
pub trait RaftService {
    
    async fn assign_group(node_id: NodeID, group_info: GroupProto);

    async fn append(group: GroupID, append: RaftMsg) -> Response<RaftMsg>;

    async fn append_response(group: GroupID, append_response: RaftMsg);

    async fn sync_snapshot(group: GroupID, snapshot: RaftMsg) -> Response<RaftMsg>;

    async fn accepted_snapshot(
        group: GroupID,
        reply_to: NodeID,
        snapshot: Snapshot,
    ) -> Response<()>;

    async fn request_vote(group: GroupID, vote_request: RaftMsg) -> Response<RaftMsg>;

    async fn heartbeat(group: GroupID, heartbeat: RaftMsg) -> Response<RaftMsg>;

    async fn batch_heartbeats(batched: BatchMessages);

    async fn heartbeat_response(group: GroupID, heartbeat_response: RaftMsg);

    async fn transfer_leader_timeout(group: GroupID, timeout: RaftMsg) -> Response<()>;

    async fn redirect_proposal(group: GroupID, propose: RaftMsg) -> Response<u64>;

    async fn redirect_transfer_leader(group: GroupID, transfer: RaftMsg) -> Response<()>;

    async fn redirect_read_index(group: GroupID, readindex: RaftMsg) -> Response<RaftMsg>;

    async fn report_snapshot(
        group: GroupID,
        reporter_id: NodeID,
        leader: NodeID,
        status: String,
    ) -> Response<()>;
}
