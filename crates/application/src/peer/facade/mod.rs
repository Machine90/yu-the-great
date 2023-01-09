pub mod local;
pub mod mailbox;

use super::{
    process::{tick::Ticked},
    PeerID,
};
use crate::async_trait;
use crate::protos::{prelude::BatchConfChange, raft_payload_proto::StatusProto};
use crate::{mailbox::RaftEndpoint, YuError, Yusult};
use common::protocol::{read_state::ReadState, NodeID, proposal::Proposal};
use consensus::prelude::raft_role::RaftRole;
use serde::Serialize;

use crate::torrent::{bincode, runtime::blocking};

/// Performs sync operation for Peer, use default or current (if exists)
/// runtime to block for process.
pub trait Facade: AbstractPeer {
    #[inline]
    fn propose_ser<P>(&self, propose: P) -> Yusult<Proposal>
    where
        P: Serialize,
    {
        let propose = bincode::serialize(&propose);
        if let Err(e) = propose {
            let err_msg = e.to_string();
            return Yusult::Err(YuError::CodecError(err_msg));
        }
        blocking(self.propose_async(propose.unwrap()))
    }

    #[inline]
    fn propose_bin<B>(&self, propose: B) -> Yusult<Proposal>
    where
        B: AsRef<[u8]>,
    {
        blocking(self.propose_async(propose.as_ref().to_vec()))
    }

    #[inline]
    fn propose_changes<C: Into<BatchConfChange>>(
        &self, 
        changes: C, 
        get_status: bool
    ) -> Yusult<Option<StatusProto>> {
        blocking(self.propose_conf_changes_async(changes.into(), get_status))
    }

    #[inline]
    fn read<INDEX>(&self, index: INDEX) -> Yusult<ReadState>
    where
        INDEX: Serialize,
    {
        let req_ctx = bincode::serialize(&index);
        if let Err(e) = req_ctx {
            let err_msg = e.to_string();
            return Yusult::Err(YuError::CodecError(err_msg));
        }
        blocking(self.read_async(req_ctx.unwrap()))
    }

    #[inline]
    fn tick(&self) -> Yusult<Ticked> {
        blocking(self.tick_async())
    }

    /// Initial an election on this peer, maybe become leader
    /// after campaign.
    #[inline]
    fn election(&self) -> Yusult<RaftRole> {
        blocking(self.election_async())
    }

    /// Request for transferring the leadership to target voter.
    /// if current peer is follower, then forward this request to
    /// it's leader. 
    #[inline]
    fn transfer_leader(&self, to: NodeID) -> Yusult<()>{
        blocking(self.transfer_leader_async(to))
    }
}

#[async_trait]
pub trait AbstractPeer {
    /// The ID (groupid, nodeid) mapped to raft peer.
    fn my_id(&self) -> PeerID;

    /// Get the endpoint info of this peer, when local peer, 
    /// it should be current Node's info.
    fn get_endpoint(&self) -> std::io::Result<RaftEndpoint>;

    async fn propose_async(&self, data: Vec<u8>) -> Yusult<Proposal>;

    /// Post changes (e.g. add voter, remove peer and add learner)
    /// to this group. Return latest status if apply changes success.
    async fn propose_conf_changes_async(
        &self, changes: BatchConfChange, 
        get_status: bool
    ) -> Yusult<Option<StatusProto>>;

    async fn propose_cmd_async(&self, cmd: Vec<u8>) -> Yusult<Proposal>;

    /// Perform read operation in raft group.
    /// ### Params
    /// * index: used to query specific content
    async fn read_async(&self, index: Vec<u8>) -> Yusult<ReadState>;

    async fn status_async(&self) -> Yusult<StatusProto>;

    /// Tick this raft node and trigger to heartbeat (as leader) or election (as follower)
    async fn tick_async(&self) -> Yusult<Ticked>;

    async fn election_async(&self) -> Yusult<RaftRole>;

    async fn transfer_leader_async(&self, to: NodeID) -> Yusult<()>;
}
