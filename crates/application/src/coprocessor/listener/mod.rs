pub mod proposal;
pub mod snapshot;
pub mod admin;

use std::{sync::{Arc}, ops::{Deref, DerefMut}};
use components::{vendor::prelude::DashMap, torrent::partitions::partition::Partition};
use consensus::prelude::raft_role::RaftRole;
use self::{proposal::RaftListener, snapshot::SnapshotListener, admin::AdminListener};
use crate::{RaftStatus, GroupID};

#[derive(Debug, Default, Clone)]
pub struct RaftContext {
    pub node_id: u64,
    pub group_id: u32,
    pub role: RaftRole,
    pub commit: u64,
    pub applied: u64,
    pub term: u64,
    pub partition: Option<Partition<()>>,
}

impl RaftContext {
    pub fn from_status(group: GroupID, status: RaftStatus<'_>) -> Self {
        Self {
            node_id: status.id,
            group_id: group, 
            role: status.role(), 
            commit: status.hard_state.commit,
            applied: status.applied_index,
            term: status.hard_state.term,
            partition: None
        }
    }
}

/// Access Control List of Listener.
#[crate::async_trait]
pub trait Acl: Sync {
    /// Is this listener accessible? default to not 0, where 0 is 
    /// a special group using to mark this group as registry.
    #[inline] async fn accessible(&self, _ctx: &RaftContext) -> bool {
        true
    }
}

#[derive(Clone)]
pub enum Listener {
    /// Used to handle read and write operation.
    Proposal(Arc<dyn RaftListener + Send + Sync>),
    /// Used to handle commands.
    Admin(Arc<dyn AdminListener + Send + Sync>),
    Snapshot(Arc<dyn SnapshotListener + Send + Sync>),
}

impl Listener {

    /// clone a new reference of listener without new it.
    pub fn fork(&self) -> Self {
        match self {
            Listener::Proposal(listener) => {
                Listener::Proposal(listener.clone())
            },
            Listener::Admin(admin) => {
                Listener::Admin(admin.clone()) 
            },
            Listener::Snapshot(transfer) => {
                Listener::Snapshot(transfer.clone()) 
            },
        }
    }

    #[inline]
    pub async fn should_execute(&self, ctx: &RaftContext) -> bool {
        match self {
            Listener::Proposal(listener) => listener.accessible(ctx).await,
            Listener::Admin(admin) => admin.accessible(ctx).await,
            Listener::Snapshot(transfer) => transfer.accessible(ctx).await,
        }
    }
}

#[derive(Default)]
pub struct Listeners {
    table: DashMap<usize, Listener>
}

impl Listeners {
    pub fn add(&self, listener: Listener) {
        let listeners = &self.table;
        listeners.insert(listeners.len() + 1, listener);
    }
}

impl Deref for Listeners {
    type Target = DashMap<usize, Listener>;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

impl DerefMut for Listeners {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.table
    }
}