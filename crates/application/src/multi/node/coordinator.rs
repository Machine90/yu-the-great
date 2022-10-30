pub mod admin;
pub mod report;
pub mod sampling;

use super::manager::NodeManager;
use crate::{storage::group_storage::GroupStorage, NodeID};
use common::metrics::sys_metrics::SysMetrics;
use components::{
    mailbox::{
        multi::{balance::NodeBalancer, federation::FederationRef},
        topo::Topo,
    },
    monitor::Monitor,
    vendor::prelude::lock::RwLock,
};
use std::sync::Arc;

pub struct NodeCoordinator<S: GroupStorage> {
    pub manager: Arc<NodeManager<S>>,
    pub(crate) balancer: NodeBalancer,
    pub(self) federation: Option<FederationRef>,
    sys_metrics: RwLock<SysMetrics>,
}

impl<S: GroupStorage> NodeCoordinator<S> {
    /// don't use it immediately without set node_manager via set_manager.
    pub fn new(manager: Arc<NodeManager<S>>, balancer: NodeBalancer) -> Self {
        let this = Self {
            manager,
            balancer,
            federation: None,
            sys_metrics: RwLock::new(SysMetrics::default()),
        };
        if let Some(metrics) = this.sampling_sys(true) {
            crate::info!("{metrics}");
        }
        this
    }

    #[inline]
    pub fn set_federation(&mut self, federation: FederationRef) {
        self.federation = Some(federation);
    }

    #[inline]
    pub fn monitor(&self) -> Option<&Monitor> {
        self.manager.coprocessor_driver().monitor()
    }

    #[inline]
    pub fn topo(&self) -> &Topo {
        &self.manager.topo
    }

    pub fn node_id(&self) -> NodeID {
        self.manager.id
    }
}
