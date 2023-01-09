use std::{collections::HashSet, sync::Arc};

use common::protos::raft_group_proto::GroupProto;
use components::{
    mailbox::{topo::Topo, PostOffice},
    monitor::MonitorConf,
};

use crate::{
    coprocessor::{
        delegation::{batch::BatchCoprocessor, strictly::LinearCoprocessor},
        builder as driver,
        listener::{proposal::RaftListener, snapshot::SnapshotListener, Listener},
        RaftCoprocessor,
    },
    engine::sched::{schedule::Schedule, scheduler::Scheduler},
    peer::{config::NodeConfig, Peer},
    storage::group_storage::GroupStorage, single::{Core, Node, schedules::ticker::SingleTicker},
};

pub struct Builder<S: GroupStorage + Clone> {
    pub(crate) cp_driver_builder: driver::Builder,
    scheduler: Scheduler,
    group: GroupProto,
    pub(crate) post_office: Option<Arc<dyn PostOffice + 'static>>,
    store: Option<S>,
    tick: bool,
}

impl<S: GroupStorage + Clone> Builder<S> {
    pub fn new(group: GroupProto, config: NodeConfig) -> Self {
        let _valid = group.validate(false);
        let topo = Topo::from_group(&group, HashSet::from([config.id]));
        let cp_driver_builder = driver::Builder::from_conf(config).with_topology(topo.clone());
        Self {
            group,
            cp_driver_builder,
            post_office: None,
            store: None,
            scheduler: Scheduler::default(),
            tick: false,
        }
    }

    #[inline]
    pub fn use_default(self) -> Self {
        let node = self.cp_driver_builder.conf.id;
        let enable_batch_read = self.cp_driver_builder.conf.enable_read_batch;
        let mut this = self.tick(true);
        if enable_batch_read {
            this = this.register_coprocessor(BatchCoprocessor::new(node));
        } else {
            this = this.register_coprocessor(LinearCoprocessor::new(node));
        }
        #[cfg(feature = "rpc_transport")]
        {
            this = this.use_rpc_transport();
        }
        this
    }

    #[inline]
    pub fn with_schedule<SC: Schedule + 'static>(mut self, schedule: SC) -> Self {
        let _ = self.scheduler.spawn(schedule);
        self
    }

    #[inline]
    pub fn tick(mut self, should_tick: bool) -> Self {
        self.tick = should_tick;
        self
    }

    #[inline]
    pub fn add_listener(mut self, listener: Listener) -> Self {
        self.cp_driver_builder = self.cp_driver_builder.add_listener(listener);
        self
    }

    #[inline]
    pub fn config_monitor(mut self, conf: MonitorConf) -> Self {
        self.cp_driver_builder = self.cp_driver_builder.config_monitor(conf);
        self
    }

    #[inline]
    pub fn add_snapshot_listener_ref(
        self,
        listener: Arc<dyn SnapshotListener + Send + Sync + 'static>,
    ) -> Self {
        self.add_listener(Listener::Snapshot(listener))
    }

    #[inline]
    pub fn add_raft_listener_ref(
        self,
        listener: Arc<dyn RaftListener + Send + Sync + 'static>,
    ) -> Self {
        self.add_listener(Listener::Proposal(listener))
    }

    #[inline]
    pub fn add_raft_listener<L: RaftListener + Send + Sync + 'static>(self, listener: L) -> Self {
        self.add_raft_listener_ref(Arc::new(listener))
    }

    #[inline]
    pub fn add_snapshot_listener<L: SnapshotListener + Send + Sync + 'static>(
        self,
        listener: L,
    ) -> Self {
        self.add_listener(Listener::Snapshot(Arc::new(listener)))
    }

    #[inline]
    pub fn register_coprocessor_ref(mut self, cop: Arc<dyn RaftCoprocessor + Send>) -> Self {
        self.cp_driver_builder = self.cp_driver_builder.register_coprocessor(cop);
        self
    }

    #[inline]
    pub fn register_coprocessor<CP>(mut self, cop: CP) -> Self
    where
        CP: RaftCoprocessor + Send + 'static,
    {
        self.cp_driver_builder = self.cp_driver_builder.register_coprocessor(Arc::new(cop));
        self
    }

    #[inline]
    pub fn customize_mailbox_provider<MP>(mut self, provider: MP) -> Self
    where
        MP: PostOffice + 'static,
    {
        let provider = Arc::new(provider);
        self.post_office = Some(provider);
        self
    }

    /// Set raft log storage via provider. The storage
    /// would be returned from provider and set to
    /// builder.
    pub fn with_raftlog_store<P>(mut self, provider: P) -> Self
    where
        P: Fn(&GroupProto) -> S,
    {
        let store = provider(&self.group);
        self.store = Some(store);
        self
    }

    fn _validate(&self) -> crate::RaftResult<()> {
        if self.post_office.is_none() {
            return Err(crate::ConsensusError::Other(
                "lack of mailbox provider, please give one via method `customize_mailbox_provider`"
                    .into(),
            ));
        }
        if self.store.is_none() {
            return Err(crate::ConsensusError::Other(
                "lack of storage, please give one via `with_storage`".into(),
            ));
        }
        Ok(())
    }

    pub fn build(mut self) -> crate::RaftResult<Node> {
        // step 1: check if setup correct
        self._validate()?;

        let group = self.group;
        let post_office = self.post_office.take().unwrap();

        // step 2: take out coprocessor driver and build it.
        let cp_driver_builder = std::mem::take(&mut self.cp_driver_builder);
        let cp_driver = Arc::new(cp_driver_builder.build(post_office)?);

        // step 3: build storage and mailbox for this peer.
        let store = self.store.take().unwrap();
        let mailbox = cp_driver.build_mailbox(group.id);
        if let Some(voters) = group.get_voters() {
            let peers = voters.iter().map(|v| (group.id, *v)).collect();
            cp_driver.establish_connections(peers);
        }

        // step 4: build raft peer with mailbox, storage and coprocessor
        let peer = Peer::assign(group, store, mailbox, cp_driver)?;

        let mut scheduler = std::mem::take(&mut self.scheduler);
        let peer = Arc::new(peer);
        if self.tick {
            // step 5: if tick it by default, spawn a tick scheduler
            let _ = scheduler.spawn_async(SingleTicker::new(peer.clone()));
        }
        // final step: build Node with schedule and peer
        Ok(Node {
            core: Arc::new(Core {
                peer,
                scheduler: Arc::new(scheduler),
            }),
        })
    }
}

pub mod provider {
    use crate::store::memory::{storage_impl::MemoryStorage, store::PeerMemStore};
    use common::protos::raft_group_proto::GroupProto;

    pub fn mem_raftlog_store(group: &GroupProto) -> PeerMemStore {
        let GroupProto { id, confstate, .. } = &group;
        let mem_store = MemoryStorage::new();
        if let Some(cs) = confstate {
            mem_store.initialize_with_conf_state(cs.clone());
        }
        PeerMemStore::new(*id, mem_store)
    }
}
