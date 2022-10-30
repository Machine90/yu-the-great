use common::{protos::raft_group_proto::{GroupProto, EndpointProto}};
use components::{
    mailbox::{api::GroupMailBox, topo::{Topo}, RaftEndpoint}, 
    storage::{group_storage::GroupStorage, multi_storage::{MultiStore, MultiStorage}}, 
    torrent::{topology::node::Node, partitions::key::Key}
};
use std::{sync::Arc, collections::HashSet, convert::TryInto};

use crate::{
    PeerID, NodeID,
    coprocessor::driver::CoprocessorDriver,
    peer::{config::NodeConfig, Peer},
    RaftResult,
    ConsensusError,
};

pub enum Moditication {
    Insert(CreatedPeer),
    Remove,
    ClearRange { from: Key, to: Key }
}

pub struct CreatedPeer {
    pub peer: Peer,
    pub voters: Vec<RaftEndpoint>,
    pub group: GroupProto
}

/// The "PeerAssiner" used to create a group-specified "peer"
/// on this node, once we attempt to create a "peer", before 
/// init it's raft group, there has some resources must be assigned:
/// * mailbox: the mailbox was setup for this peer, which used 
/// to connect to other peers in the group.
/// * multi_storage: the storage that stored groups info of this node,
/// and could be used to assigned a specified-group storage for this 
/// "peer".
/// * coprocessor: used to help to handle some process when raft stepping 
/// a message.
#[derive(Clone)]
pub struct PeerAssigner<S: GroupStorage> {
    /// used to assign coprocessor driver for raft peer.
    pub(crate) coprocessor: Arc<CoprocessorDriver>,
    /// used to assign storage for raft peer.
    pub(crate) multi_storage: MultiStore<S>
}

impl<S: GroupStorage + Clone> PeerAssigner<S> {
    pub fn new<MS>(
        cp_driver: CoprocessorDriver,
        multi_storage: MS
    ) -> Self
    where 
        MS: MultiStorage<S> + Send + Sync + 'static
    {
        Self {
            coprocessor: Arc::new(cp_driver), 
            multi_storage: Arc::new(multi_storage)
        }
    }

    /// Create peer from group but not register to topo and multi-store until
    /// `apply` to node.
    pub fn new_peer(&self, mut group: GroupProto) -> RaftResult<CreatedPeer> {
        let group_id = group.id;
        let to_persist = group.clone();

        let cp = self.coprocessor.as_ref();
        let mailbox: Arc<dyn GroupMailBox> = cp.build_mailbox(group_id);

        let voters = group
            .get_voters()
            .ok_or(ConsensusError::Other("group must have confstate".into()))?;
        let endpoints = group.take_endpoints();

        let store = self.multi_storage.build_store(&mut group)?;
        let peer = Peer::assign(
            group, 
            store, 
            mailbox, 
            self.coprocessor.clone()
        )?;

        let voters = fast_convert(endpoints, voters, cp.topo())?;
        Ok(CreatedPeer { peer, voters, group: to_persist })
    }

    pub fn apply(
        &self, 
        created_peer: CreatedPeer
    ) -> (Peer, HashSet<PeerID>) {
        let CreatedPeer { voters, peer, group } = created_peer;
        let belong_group = peer.get_group_id();
        self.multi_storage.persist_group(group);
        let voters = self.coprocessor.apply_voters(
            belong_group, 
            voters
        );
        (peer, voters)
    }

    /// Assign the group with topology immediately, unlike `new_peer`, when this method create 
    /// raft group success, the group and voters will be registered to `topo` in place, 
    /// and also group store of this raft will be add to `multi_store`.
    pub fn assign(
        &self, 
        mut group: GroupProto,
        persist: bool
    ) -> RaftResult<(Peer, HashSet<PeerID>)> {
        let group_id = group.id;

        let voters = group
            .get_voters()
            .ok_or(ConsensusError::Other("group must have confstate".into()))?;

        // step 1: build and maybe persist group store.
        let storage = if persist {
            let persist = group.clone();
            self.multi_storage.assign_group(persist)?
        } else {
            self.multi_storage.build_store(&mut group)?
        };
        let mailbox: Arc<dyn GroupMailBox> = self
            .coprocessor
            .build_mailbox(group_id);
        let endpoints = group.take_endpoints();
        
        // peer only draw attention to group's id and key range.
        let peer = Peer::assign(
            group, 
            storage, 
            mailbox, 
            self.coprocessor.clone()
        )?;

        let added = self.coprocessor.apply_voters(
            group_id, 
            fast_convert(endpoints, voters, self.coprocessor.topo())?
        );
        Ok((peer, added))
    }

    #[inline]
    pub fn conf(&self) -> &NodeConfig {
        &self.coprocessor.conf()
    }

    #[inline]
    pub fn node_id(&self) -> NodeID {
        self.coprocessor.node_id()
    }

    #[inline]
    pub fn establish_connections(&self, peers: HashSet<PeerID>) {
        self.coprocessor.establish_connections(peers);
    }

    #[inline]
    pub fn topo(&self) -> &Topo {
        self.coprocessor.topo()
    }
}

/// Try convert endpoint to node, and filling nodes of voters by lookup from 
/// topo.  
fn fast_convert(
    endpoints: Vec<EndpointProto>, 
    mut voters: HashSet<NodeID>, 
    topo: &Topo
) -> RaftResult<Vec<RaftEndpoint>> {
    let mut nodes = vec![];
    for endpoint in endpoints { 
        let node_id = endpoint.id;
        if !voters.contains(&node_id) {
            continue;
        }
        voters.remove(&node_id);
        let node = endpoint.try_into();
        if let Err(e) = node {
            return Err(ConsensusError::Io(e));
        }
        nodes.push(node.unwrap());
    }
    for absent in voters {
        if topo.contained_node(&absent) {
            nodes.push(Node {
                id: absent,
                ..Default::default()
            });
            continue;
        }
    }
    Ok(nodes)
}
