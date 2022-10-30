use common::errors::application::{YuError, Yusult};
use components::{torrent::partitions::index::mvcc::Mvcc};

use crate::{
    multi::node::LocalPeers, GroupID, NodeID, 
    peer::facade::local::LocalPeer
};

/// Local router of groups on this node.
#[derive(Clone)]
pub struct NodeRouter {
    pub id: NodeID,
    /// Index table for partitions of key to group_id.
    pub(super) partitions: Mvcc<GroupID>,
    /// local groups on this node.
    pub(crate) peers: LocalPeers,
}

impl NodeRouter {

    #[inline]
    pub fn partitions(&self) -> &Mvcc<GroupID> {
        &self.partitions
    }

    /// Try to find peer on this node in specific group.
    /// otherwise return `YuError::NotSuchPeer` error
    #[inline]
    pub fn find_peer(&self, group_id: GroupID) -> Yusult<LocalPeer> {
        let peer = self.peers.get(&group_id);
        if peer.is_none() {
            let node_id = self.id;
            return Err(YuError::NotSuchPeer(group_id, node_id));
        }
        let peer = peer.unwrap();
        Ok(peer.clone())
    }
}
