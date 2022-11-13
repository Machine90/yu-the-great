pub mod coordinator;
pub mod facade;
pub mod manager;

pub type LocalPeers = Arc<DashMap<GroupID, LocalPeer>>;

use self::coordinator::NodeCoordinator;
use self::manager::NodeManager;
use crate::peer::facade::local::LocalPeer;
use crate::vendor::prelude::*;
use crate::{engine::sched::scheduler::Scheduler, storage::group_storage::GroupStorage};
use common::protocol::GroupID;
use components::mailbox::RaftEndpoint;
use components::torrent::dams::Terminate;
use std::{ops::Deref, sync::Arc};

pub struct Node<S: GroupStorage> {
    pub(crate) node_manager: Arc<NodeManager<S>>,
    pub(crate) coordinator: Option<Arc<NodeCoordinator<S>>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) terminate: Terminate,
}

impl<S: GroupStorage> Node<S> {
    
    #[inline]
    pub fn coordinator(&self) -> Option<Arc<NodeCoordinator<S>>> {
        self.coordinator.clone()
    }

    /// Get the endpoint information of this node.
    #[inline]
    pub fn endpoint(&self) -> &RaftEndpoint {
        &self.coprocessor_driver().endpoint
    }

    #[inline]
    pub fn stop(&self) {
        self.terminate.stop();
    }
}

impl<S: GroupStorage> Deref for Node<S> {
    type Target = NodeManager<S>;

    fn deref(&self) -> &Self::Target {
        self.node_manager.as_ref()
    }
}
