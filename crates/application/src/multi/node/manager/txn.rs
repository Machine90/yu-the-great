//! It's just a Demo or prototype, replace this implementation later
pub(super) mod assign_group;

use components::mailbox::multi::balance::Notification;
use components::storage::group_storage::GroupStorage;
use components::torrent::partitions::{index::mvcc::version::Version};
use std::collections::{HashSet};

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

    /// Commit transaction, maybe some groups should 
    /// be compacted after commit.
    pub async fn commit(self) -> Vec<(GroupID, Notification)> {
        let Self {
            version,
            peers,
            peer_assigner,
            modification,
            ..
        } = self;

        let mut all_voters = HashSet::new();
        let mut notification = Vec::new();
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
                            let (from, to) = peer.group_range();
                            let noti = Notification::ScaleDown {
                                key_range: from..to
                            };
                            notification.push((group, noti));
                        }
                    }
                    Moditication::ScaleUp { new_from, new_to } => {
                        if let Some(peer) = peers.get(&group) {
                            let range = new_from..new_to;
                            let noti = Notification::ScaleUp {
                                key_range: range.clone()
                            };
                            notification.push((group, noti));
                            peer.update_key_range(range);
                        }
                    }
                    Moditication::Remove => {
                        if let Some((_, peer)) = peers.remove(&group) {
                            let (from, to) = peer.group_range();
                            let noti = Notification::ClearRange { 
                                group, key_range: from..to 
                            };
                            notification.push((group, noti));
                            peer.clear().await;
                        }
                    }
                    Moditication::MergeTo(merged_group) => {
                        if let Some((_, peer)) = peers.remove(&group) {
                            let noti = Notification::ChangeGroup { to: merged_group };
                            notification.push((group, noti));
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
        notification
    }

    pub async fn rollback(self) {
        self.trace("rollback");
    }

    #[inline]
    pub(self) fn trace(&self, action: &str) {
        crate::debug!("{:?} do {action}.", self.version);
    }
}
