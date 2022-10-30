use crate::multi::node::coordinator::NodeCoordinator;
use crate::multi::node::{
    manager::{peer_assigner::PeerAssigner, NodeManager},
    Node,
};
use crate::multi::schedules::reporter::NodeReporter;
use crate::multi::schedules::{checker::GroupChecker, ticker::BatchTicker};
use crate::vendor::prelude::*;
use crate::{
    coprocessor::delegation::batch::BatchCoprocessor,
    engine::sched::schedule::{Schedule, ScheduleAsync},
};
use crate::{
    coprocessor::{
        builder as driver,
        delegation::strictly::LinearCoprocessor,
        listener::{proposal::RaftListener, Listener},
    },
    engine::sched::scheduler::Scheduler,
    peer::config::NodeConfig,
    storage::group_storage::GroupStorage,
};
use components::mailbox::multi::balance::BalanceHelper;
use components::{
    mailbox::{
        multi::{
            balance::NodeBalancer,
            federation::{Federation, FederationRef},
        },
        PostOffice,
    },
    monitor::MonitorConf,
    storage::multi_storage::{MultiStorage, MultiStore},
    torrent::dams::Terminate,
};
use std::sync::Arc;

/// Multi-groups node builder, help to build
/// the [Node](crate::multi::node::Node) easier.
pub struct Builder<S: GroupStorage> {
    multi_store: Option<MultiStore<S>>,
    pub(crate) cp_driver: driver::Builder,
    pub(crate) post_office: Option<Arc<dyn PostOffice + 'static>>,
    scheduler: Scheduler,
    tick: bool,
    node_balancer: Option<NodeBalancer>,
    federation: Option<FederationRef>,
}

impl<S: GroupStorage + Clone> Builder<S> {
    pub fn new(node_config: NodeConfig) -> Self {
        let cp_driver = driver::Builder::from_conf(node_config);
        Self {
            multi_store: None,
            cp_driver,
            post_office: None,
            tick: false,
            scheduler: Scheduler::default(),
            node_balancer: None,
            federation: None,
        }
    }

    /// Use default configuration and coprocessor
    pub fn use_default(mut self) -> Self {
        self.tick = true;
        let node = self.cp_driver.conf.id;

        let enable_batch_read = self.cp_driver.conf.consensus_config.enable_read_batch;
        self.cp_driver = if enable_batch_read {
            self.cp_driver
                .register_coprocessor(Arc::new(BatchCoprocessor::new(node)))
        } else {
            self.cp_driver
                .register_coprocessor(Arc::new(LinearCoprocessor::new(node)))
        };
        // the multi-raft default to sampling each group
        self = self.config_monitor(MonitorConf {
            enable_group_sampling: true,
            ..Default::default()
        });
        #[cfg(feature = "rpc_transport")]
        {
            self = self.use_rpc_transport();
        }
        self
    }

    /// Should tick the multi-raft by using `BatchTicker`
    #[inline]
    pub fn tick(mut self, should_tick: bool) -> Self {
        self.tick = should_tick;
        self
    }

    pub fn add_raft_listener<L: RaftListener + Send + Sync + 'static>(
        mut self,
        listener: L,
    ) -> Self {
        self.cp_driver = self
            .cp_driver
            .add_listener(Listener::Proposal(Arc::new(listener)));
        self
    }

    pub fn add_raft_listener_ref(
        mut self,
        listener: Arc<dyn RaftListener + Send + Sync + 'static>,
    ) -> Self {
        self.cp_driver = self.cp_driver.add_listener(Listener::Proposal(listener));
        self
    }

    #[inline]
    pub fn register_balancer<B: BalanceHelper + Send + Sync + 'static>(
        mut self,
        balancer: B,
    ) -> Self {
        self.node_balancer = Some(Arc::new(balancer));
        self
    }

    #[inline]
    pub fn config_monitor(mut self, conf: MonitorConf) -> Self {
        self.cp_driver = self.cp_driver.config_monitor(conf);
        self
    }

    #[inline]
    pub fn register_balancer_ref(mut self, balancer: NodeBalancer) -> Self {
        self.node_balancer = Some(balancer);
        self
    }

    /// Set the raft_log storage impl for multi-raft groups.
    #[inline]
    pub fn with_raftlog_store<MS: MultiStorage<S> + Send + Sync + 'static>(
        mut self,
        store: MS,
    ) -> Self {
        self.multi_store = Some(Arc::new(store));
        self
    }

    /// Customize the mailbox provider, this often require for customize
    /// group mailbox implement.
    #[inline]
    pub fn customize_mailbox_provider<MP>(mut self, provider: MP) -> Self
    where
        MP: PostOffice + 'static,
    {
        let provider = Arc::new(provider);
        self.post_office = Some(provider);
        self
    }

    #[inline]
    pub fn with_schedule<SC: Schedule + 'static>(mut self, schedule: SC) -> Self {
        let _ = self.scheduler.spawn(schedule);
        self
    }

    #[inline]
    pub fn with_schedule_async<SC: ScheduleAsync + 'static>(mut self, schedule: SC) -> Self {
        let _ = self.scheduler.spawn_async(schedule);
        self
    }

    /// Federation is used to manage the multi-raft cluster
    pub fn with_federation<F>(mut self, federation: F) -> Self
    where
        F: Federation + 'static,
    {
        self.federation = Some(Arc::new(federation));
        self
    }

    pub fn with_federation_ref(mut self, federation: FederationRef) -> Self {
        self.federation = Some(federation);
        self
    }

    pub fn validate(&self) {
        assert!(
            self.multi_store.is_some(),
            "`storage` must be assigned, require it for restoring peers"
        );
        assert!(
            self.post_office.is_some(),
            "mailbox provider must be assigned, or use default instead"
        );
        let listeners = self.cp_driver.listeners_num();
        if !self.cp_driver.has_coprocessor() && listeners > 0 {
            warn!("there has {:?} listener but not coprocessor set", listeners);
        }
    }

    pub fn build(mut self) -> Node<S> {
        self.validate();

        // multi store include metadata (groups and partitions) of this node.
        let multi_store = self.multi_store.take().unwrap();
        // used to create mailbox for groups
        let post_office = self.post_office.take().unwrap();

        let cp_builder = std::mem::take(&mut self.cp_driver);
        // take scheduler out.
        let mut scheduler = std::mem::take(&mut self.scheduler);

        // register coordinator as `AdminListener` to
        // to coprocessor driver.
        let cp_driver = cp_builder.build(post_office.clone()).unwrap();
        let cp_driver = Arc::new(cp_driver);

        let node_id = cp_driver.node_id();

        // =>> Step 1: create peer assigner, used to assign group on each node.
        let peer_assigner: PeerAssigner<S> = PeerAssigner {
            coprocessor: cp_driver.clone(),
            multi_storage: multi_store,
        };

        // =>> Step 2: try to restore peers on this node from metadata.
        let federation = self.federation.take();
        let should_report = federation.is_some();

        let node_manager = Arc::new(NodeManager::load(peer_assigner, should_report));
        // =>> Step 3: If balancer registered, set it to coordinator.
        let coordinator = if let Some(balancer) = self.node_balancer.take() {
            let mut coordinator = NodeCoordinator::new(
                node_manager.clone(), 
                balancer.clone()
            );

            // ==> Step 3-1: set federation to coordinator if configured.
            if let Some(federation) = federation.clone() {
                coordinator.set_federation(federation);                
            }
            let coordinator = Arc::new(coordinator);

            if should_report {
                let _ = scheduler.spawn_async(NodeReporter::new(
                    coordinator.clone()
                ));
            }

            if balancer.conf().enable_schedule {
                // =>> Step 3-2: schedule sampler with coordinator.
                let _ = scheduler.spawn_async(GroupChecker::new(coordinator.clone()));
            }
            // =>> Step 3-3: then register coordinator as `Admin` listener
            // to receive and handle commands.
            cp_driver.add_listener(Listener::Admin(coordinator.clone()));
            Some(coordinator)
        } else {
            None
        };

        // =>> Step 4: if enable tick node.
        if self.tick {
            // then tick hearbeat/election on this node.
            let node_manager = node_manager.clone();
            let _ = scheduler.spawn_async(BatchTicker::new(
                node_id, 
                node_manager
            ));
        }

        Node {
            coordinator,
            node_manager,
            scheduler: Arc::new(scheduler),
            terminate: Terminate::new(),
        }
    }
}
