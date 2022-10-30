//! It's just a Demo or prototype, replace this implementation later
pub(super) mod assign_group;

use components::storage::group_storage::GroupStorage;
use components::torrent::partitions::{index::mvcc::version::Version, key::Key};
use std::collections::{HashSet, VecDeque};

use super::peer_assigner::{Moditication, Moditications, PeerAssigner};
use super::NodeManager;
use crate::multi::node::LocalPeers;
use crate::{peer::facade::local::LocalPeer, GroupID};

pub struct Transaction<S: GroupStorage> {
    pub(super) version: Version<GroupID>,
    modification: Moditications, // Should be modifications
    pub(super) peers: LocalPeers,
    pub(super) peer_assigner: PeerAssigner<S>,
}

impl<S: GroupStorage + Clone> Transaction<S> {
    pub async fn new(manager: &NodeManager<S>) -> Self {
        // create a transaction with partition mvcc version
        let version = manager.partitions().new_version();
        Self {
            version,
            modification: Moditications::new(),
            peers: manager.peers.clone(),
            peer_assigner: manager.peer_assigner.clone(),
        }
    }

    pub async fn commit(self) -> VecDeque<(GroupID, Key, Key)> {
        let Self {
            version,
            peers,
            peer_assigner,
            modification,
            ..
        } = self;

        let mut all_voters = HashSet::new();
        let mut clear_partitions = VecDeque::new();
        for (group, changes) in modification.edits {
            for change in changes {
                match change {
                    Moditication::Insert(peer) => {
                        let (peer, voters) = peer_assigner.apply(peer);
                        all_voters.extend(voters);
                        peers.insert(group, LocalPeer::new(peer));
                    }
                    Moditication::ScaleDown { new_from } => {
                        if let Some(peer) = peers.get(&group) {
                            peer.set_from_key(new_from);
                        }
                    }
                    Moditication::ScaleUp { new_from, new_to } => {
                        if let Some(peer) = peers.get(&group) {
                            peer.update_key_range(new_from..new_to);
                        }
                    }
                    Moditication::Remove => {
                        if let Some((_, peer)) = peers.remove(&group) {
                            let (from, to) = peer.group_range();
                            clear_partitions.push_back((group, from, to));
                            peer.clear().await;
                        }
                    }
                    Moditication::MergeTo(_merged_group) => {
                        if let Some((_, peer)) = peers.remove(&group) {
                            // TODO list: 
                            // 1. notify to business
                            // 2. handle unapply entries of these peers.
                            peer.clear().await;
                        }
                    }
                }
            }
        }
        version.commit();

        if !all_voters.is_empty() {
            // then establish connect to added peers after commit.
            peer_assigner.establish_connections(all_voters);
        }
        clear_partitions
    }

    pub async fn rollback(self) {
        self.trace("rollback");
    }

    #[inline]
    pub(self) fn trace(&self, action: &str) {
        crate::debug!("{:?} do {action}.", self.version);
    }
}
