use std::sync::{Arc, atomic::{AtomicPtr, Ordering}};

use consensus::{raft_node::{RaftNode}, prelude::raft_role::RaftRole};
use common::storage::Storage;
use crate::{tokio::{sync::{RwLock, RwLockWriteGuard, RwLockReadGuard}}};
use crate::peer::RaftPeer;

pub type WLockRaft<'a> = RwLockWriteGuard<'a, RaftPeer>;
pub type RLockRaft<'a> = RwLockReadGuard<'a, RaftPeer>;

pub struct RaftGroup<S: Storage> {
    raft_node: Arc<RwLock<RaftNode<S>>>
}

impl<S> RaftGroup<S> where S: Storage {
    pub fn new(mut node: RaftNode<S>) -> Self {
        Self {
            raft_node: Arc::new(RwLock::new(node))
        }
    }

    /// Acquire raft async write-lock in tokio Runtime.
    #[inline] pub async fn wl_raft(&self) -> RwLockWriteGuard<'_, RaftNode<S>> {
        self.raft_node.write().await
    }

    /// Acquire raft async read-lock in tokio Runtime.
    #[inline] pub async fn rl_raft(&self) -> RwLockReadGuard<'_, RaftNode<S>> {
        self.raft_node.read().await
    }

    #[inline] pub async fn role(&self) -> RaftRole {
        self.rl_raft().await.role()
    }

    #[inline] pub async fn leader_id(&self) -> u64 {
        self.rl_raft().await.leader_id()
    }

    #[inline]
    pub async fn campaigned(&self) -> bool {
        self.raft_node.read().await.campaigned()
    }
}
