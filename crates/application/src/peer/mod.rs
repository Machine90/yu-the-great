//! Module of raft peer, it's the core of the application
//! which implement full features of the raft algorithm includes
//! * Election
//! * Propose (both normal LogEntry and Changes) & Append
//! * ReadIndex
//! * Heartbeat
//! * Transfer leader

pub mod config;
pub mod facade;

#[allow(unused)]
mod pipeline;
pub(crate) mod process;
#[allow(unused)]
mod raft_group;

use self::{config::NodeConfig, raft_group::raft_node::RaftGroup};
use crate::{
    GroupID, NodeID, PeerID,
    coprocessor::driver::CoprocessorDriver,
    mailbox::{
        api::GroupMailBox,
        RaftEndpoint,
    },
    storage::group_storage::GroupStorage,
    tokio::sync::RwLock,
    RaftResult,
};
use common::protos::{
    raft_group_proto::GroupProto, raft_log_proto::{ConfState}, 
    raft_payload_proto::StatusProto
};
use components::{torrent::{partitions::{key::Key, partition::Partition}}, monitor::Monitor};
use consensus::{raft_node::RaftNode};
use std::{fmt::Debug, ops::Deref, sync::Arc};

/// Raw node with `GroupStorage` trait.
pub type RaftPeer = RaftNode<Box<dyn GroupStorage>>;
/// The `Raft` is a consensus group in the raft.
/// it's the perspective of current peer in the group.
pub type Raft = Arc<RaftGroup<Box<dyn GroupStorage>>>;

pub struct Peer {
    core: Arc<Core>,
}

impl Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peer")
            .field("node_id", &self.node_id())
            .field("group_id", &self.get_group_id())
            .field("endpoint", &self.endpoint())
            .finish()
    }
}

impl Deref for Peer {
    type Target = Core;

    fn deref(&self) -> &Self::Target {
        self.core.as_ref()
    }
}

impl Peer {

    pub fn assign<S: GroupStorage + Clone>(
        group: GroupProto,
        storage: S,
        mailbox: Arc<dyn GroupMailBox>,
        cp_driver: Arc<CoprocessorDriver>
    ) -> RaftResult<Self> {
        let rstore = Box::new(storage.clone());
        let wstore = Arc::new(storage);
        Self::_assign_internal(group, rstore, wstore, mailbox, cp_driver)
    }

    fn _assign_internal(
        group: GroupProto,
        read_store: Box<dyn GroupStorage>,
        write_store: Arc<dyn GroupStorage>,
        mailbox: Arc<dyn GroupMailBox>,
        cp_driver: Arc<CoprocessorDriver>
    ) -> RaftResult<Self> {
        let conf = cp_driver.conf().consensus_config.clone();
        let raft = RaftNode::new(read_store, conf)?;

        let GroupProto { id, from_key, to_key, .. } = group;
        let partition = Partition::from_range(
            from_key..to_key, ()
        );
        Ok(Self {
            core: Arc::new(Core {
                group_id: id,
                partition: RwLock::new(partition),
                raft_group: Arc::new(RaftGroup::new(raft)),
                mailbox,
                write_store,
                coprocessor_driver: cp_driver
            }),
        })
    }
}

pub struct Core {
    group_id: GroupID,
    partition: RwLock<Partition<()>>,
    pub raft_group: Raft,
    pub mailbox: Arc<dyn GroupMailBox>,
    write_store: Arc<dyn GroupStorage>,
    coprocessor_driver: Arc<CoprocessorDriver>
}

impl Core {
    #[inline]
    pub fn get_id(&self) -> PeerID {
        (self.get_group_id(), self.node_id())
    }

    #[inline]
    pub fn conf(&self) -> &NodeConfig {
        &self.coprocessor_driver.conf()
    }

    #[inline]
    pub fn node_id(&self) -> NodeID {
        self.coprocessor_driver.node_id()
    }

    #[inline]
    pub fn endpoint(&self) -> &RaftEndpoint {
        &self.coprocessor_driver.endpoint
    }

    /// Only clone id, from_key, to_key and confstate from group.
    #[inline]
    pub async fn build_group(&self) -> GroupProto {
        let p = self.partition.read().await;
        let (from_key, to_key) = (
            p.from_key.as_left().to_vec(),
            p.from_key.as_right().to_vec()
        );
        drop(p);
        GroupProto {
            id: self.group_id,
            from_key, to_key,
            ..Default::default()
        }
    }

    #[inline]
    pub async fn set_from_key<K: AsRef<[u8]>>(&self, from: K) {
        *&mut self.partition.write().await.from_key = Key::left(from);
    }

    /// Sync the group detial with endpoints to the specific peer. This method
    /// called when leader figure out that some peer has lost connection.
    pub async fn sync_with(&self, peer_id: PeerID) {
        let (group_id, follower) = peer_id;
        let mut group = self.build_group().await;
        let mut endpoints = vec![];

        let topo = self.mailbox.topo();
        if topo.is_none() {
            crate::warn!(
                "group info would not sync to peer {:?} since it's absent",
                peer_id
            );
            return;
        }
        let network = topo.unwrap();
        let voters = self.rl_raft().await.status().all_voters();
        let mut voter_vec = vec![];
        if let Some(voters) = voters {
            for voter in voters {
                voter_vec.push(voter);
                if follower == voter {
                    continue;
                }
                let node = network.get_node(&group_id, &voter);
                if node.is_none() {
                    continue;
                }
                let node = node.unwrap();
                endpoints.push(node.as_ref().into());
            }
        }
        group.endpoints = endpoints;
        group.confstate = Some(ConfState {
            voters: voter_vec,
            ..Default::default()
        });
        self.mailbox.sync_with(follower, group).await;
    }

    #[inline]
    pub fn get_group_id(&self) -> GroupID {
        self.group_id
    }

    #[inline]
    pub fn coprocessor_driver(&self) -> &Arc<CoprocessorDriver> {
        &self.coprocessor_driver
    }

    #[inline]
    pub fn monitor(&self) -> Option<&Monitor> {
        self.coprocessor_driver.monitor()
    }

    pub async fn status(&self, detail: bool) -> StatusProto {
        let mut status: StatusProto = self.rl_raft().await.status().into();
        status.set_group_id(self.group_id);
        if !detail {
            return status;
        }
        if let Some(topo) = self.mailbox.topo() {
            topo.get_topo().get_group_mut(&self.group_id).map(|group| {
                let mut nodes = vec![];
                group.scan_nodes(|node| {
                    nodes.push(node.clone());
                    true
                });
                status.append_endpoints(nodes);
            });
        }
        status
    }
}

impl Deref for Core {
    type Target = Raft;

    fn deref(&self) -> &Self::Target {
        &self.raft_group
    }
}