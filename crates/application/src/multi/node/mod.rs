pub mod coordinator;
pub mod facade;
pub mod manager;

pub type LocalPeers = Arc<DashMap<GroupID, LocalPeer>>;

use self::manager::NodeManager;
use crate::peer::facade::local::LocalPeer;
use crate::vendor::prelude::*;
use crate::{engine::sched::scheduler::Scheduler, storage::group_storage::GroupStorage};
use common::protocol::GroupID;
use components::torrent::dams::Terminate;
use std::{ops::Deref, sync::Arc};

pub struct Node<S: GroupStorage> {
    pub(crate) node_manager: Arc<NodeManager<S>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) terminate: Terminate,
}

impl<S: GroupStorage> Deref for Node<S> {
    type Target = NodeManager<S>;

    fn deref(&self) -> &Self::Target {
        self.node_manager.as_ref()
    }
}
