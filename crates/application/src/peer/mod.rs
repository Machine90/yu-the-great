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
    RaftResult,
};
use common::{
    protos::{
        raft_group_proto::GroupProto, 
        raft_payload_proto::StatusProto
    }, 
    vendor::prelude::lock::RwLock, 
    storage::Storage
};
use components::{torrent::{partitions::{key::Key, partition::Partition}, runtime}, monitor::Monitor, storage::ReadStorage};
use consensus::{raft_node::RaftNode};

use std::{
    fmt::Debug, 
    ops::{Deref},
    sync::Arc, cmp
};

#[cfg(feature = "multi")]
use std::ops::Range;

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
        store: Arc<dyn GroupStorage>,
        mailbox: Arc<dyn GroupMailBox>,
        cp_driver: Arc<CoprocessorDriver>
    ) -> RaftResult<Self> {
        let mut conf = cp_driver.conf().consensus_config.clone();

        if let Some(mut applied) = group.get_applied().or(store.get_applid()) {
            // applied should larger than first index
            let first_idx = store.first_index()?;
            let persist = store.last_index()?;
            let init_state = store.initial_state()?;
            let commit = init_state.hard_state.commit;
            let last_idx = cmp::min(commit, persist);

            if first_idx > applied {
                crate::warn!("initial last_applied should never less than first index");
                applied = first_idx;
            } else if applied > last_idx {
                crate::warn!("initial last_applied should never larger than min(commit, persist)");
                applied = last_idx;
            }
            crate::debug!(
                "peer of group-{} node-{} initial with entries [{first_idx}, {persist}], commit: {commit}, applied: {applied}",
                group.id, conf.id,
            );
            conf.applied = applied;
        }
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
                write_store: store,
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

    pub(crate) async fn clear(&self) {
        // step 1: remove group from topology
        if let Some(topo) = self.mailbox.topo() {
            topo.remove_group(&self.group_id);
        }

        // step 2: clear group from coprocessor
        self.coprocessor_driver().on_remove_group(self.group_id);

        // step 3: compact raft log to applied.
        self.compact_raft_log(false).await;
    }

    pub async fn compact_raft_log(&self, should_async: bool) {
        if let Some(raft) = self.try_rl_raft() {
            let applied = raft.status().applied_index;
            self._compact_raft_log(applied, should_async).await
        } else {
            crate::debug!("compact raft log failure, could not acquire lock of raft");
        }
    }

    async fn _compact_raft_log(&self, to: u64, should_async: bool) {
        if should_async {
            let store = self.write_store.clone();
            let group_id = self.group_id;
            runtime::spawn_blocking(move || {
                if let Err(e) = store.group_compact(group_id, to) {
                    crate::warn!("compact raft log async failure, see: {:?}", e);
                }
            });
        } else {
            if let Err(e) = self.write_store.group_compact(self.group_id, to) {
                crate::warn!("compact raft log failure, see: {:?}", e);
            }
        }
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
    pub fn group_range(&self) -> (Key, Key) {
        let p = self.partition.read();
        let range = (
            p.from_key.clone(),
            p.to_key.clone()
        );
        range
    }

    #[cfg(feature = "multi")]
    #[inline]
    pub(crate) fn set_from_key(&self, from: Key) {
        *&mut self.partition.write().from_key = from;
    }

    #[cfg(feature = "multi")]
    #[inline]
    pub(crate) fn update_key_range(&self, range: Range<Key>) {
        let Range { start, end } = range;
        let mut partition = self.partition.write();
        partition.from_key = start;
        partition.to_key = end;
    }

    pub fn group_info(&self) -> Option<GroupProto> {
        let group_id = self.group_id;

        let topo = self.mailbox.topo();
        if topo.is_none() {
            crate::warn!("topology of this peer is absent");
            return None;
        }
        let network = topo.unwrap();
        let voters = network.copy_group_node_ids(&group_id);
        if voters.is_none() {
            return None;
        }
        let voters = voters.unwrap();

        let mut endpoints = vec![];
        for voter in voters.iter() {
            let node = network.get_node(voter);
            if node.is_none() {
                continue;
            }
            let node = node.unwrap();
            endpoints.push(node.as_ref().into());
        }
        let (from_key, to_key) = self.group_range();
        Some(GroupProto { 
            id: self.group_id, 
            from_key: from_key.take(), 
            to_key: to_key.take(), 
            endpoints, 
            confstate: Some(voters.into()), 
            ..Default::default()
        })
    }

    /// Sync the group detial with endpoints to the specific peer. This method
    /// called when leader figure out that some peer has lost connection.
    pub async fn sync_with(&self, peer_id: PeerID) {
        let follower = peer_id.1;
        if let Some(group) = self.group_info() {
            self.mailbox.sync_with(follower, group).await;
        }
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