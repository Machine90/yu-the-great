pub mod api;
pub mod topo;
#[cfg(feature = "multi")] pub mod multi;
pub mod single;

use std::{collections::HashSet, sync::Arc};

use self::{
    api::GroupMailBox,
    topo::Topo,
};
use common::{protos::{prelude::raft_group_ext::RAFT_MAILBOX}, protocol::{NodeID, GroupID, PeerID}};
use torrent::{topology::node::Node};

/// Default to 0, coordinator group id, a special group, can't be used.
pub const COORDINATOR_GROUP_ID: GroupID = 0;
/// Indicate a raft endpoint in the cluster.
pub type RaftEndpoint = Node<NodeID>;
/// Label of raft's mailbox port.
pub const LABEL_MAILBOX: &str = RAFT_MAILBOX;

pub trait PostOffice: Send + Sync {
    /// Build a mailbox for specific group without 
    fn build_group_mailbox(&self, group: GroupID, topo: &Topo) -> Arc<dyn GroupMailBox>;

    /// Try building connection with target peers, and register these peers
    /// to global topo.
    fn establish_connection(&self, peers: HashSet<PeerID>, topo: &Topo) -> HashSet<PeerID>;

    /// Apply voters to given group, this often couple with method `build_group_mailbox`
    /// when create group success.
    fn appy_group_voters(
        &self,
        to_group: GroupID,
        blacklist: HashSet<NodeID>,
        topo: &Topo,
        nodes: Vec<RaftEndpoint>,
    ) -> HashSet<PeerID> {
        let mut success = HashSet::with_capacity(nodes.len());

        // add or get group. this perform thread-safe for group operation.
        let mut group = topo.get_or_add_group(to_group);
        for node in nodes {
            let node_id = node.id;
            if blacklist.contains(&node_id) {
                // ignore this node, don't add to topology.
                continue;
            }
            if group.get(&node_id).is_none() {
                group = group.add(node);
                success.insert((to_group, node_id));
            }
        }
        success
    }
}
