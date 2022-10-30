pub mod peer_assigner;
pub mod router;
pub mod txn;

use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Instant, Duration};

use self::peer_assigner::PeerAssigner;
use self::router::NodeRouter;
use self::txn::Transaction;
use crate::coprocessor::driver::CoprocessorDriver;
use crate::engine::sched::scheduler::Scheduler;
use crate::peer::config::NodeConfig;
use crate::protos::raft_group_proto::GroupProto;
use crate::{mailbox::topo::Topo, peer::facade::local::LocalPeer};
use crate::{storage::group_storage::GroupStorage};
use common::errors::application::{Yusult, YuError};
use common::protocol::GroupID;
use components::storage::multi_storage::{Filter, MultiStore};
use components::torrent::partitions::{
    key::Key, partition::Partition, 
    index::{sparse::Sparse, mvcc::{timeline::Timeline, Mvcc}}
};
use components::vendor::prelude::DashSet;
use consensus::prelude::RaftRole;
use crate::{error};

use super::LocalPeers;

pub struct NodeManager<S: GroupStorage> {
    /// topology is a important role in the multi-raft, it's maintain
    /// the network connections and peer address in a container.
    pub topo: Topo,
    peer_assigner: PeerAssigner<S>,
    node_router: NodeRouter,
    /// once a peer:
    /// * became leader 
    /// 
    /// node should report this group to federation (if exists)
    pub(crate) report_list: DashSet<GroupID>,
    pub should_report: bool,
}

impl<S: GroupStorage> Deref for NodeManager<S> {
    type Target = NodeRouter;

    fn deref(&self) -> &Self::Target {
        &self.node_router
    }
}

impl<S: GroupStorage + Clone> NodeManager<S> {
    /// Try to load `NodeManager` from `multi_store` directly before
    /// node manager can provide service, this equal to `new`
    /// and `reload` in runtime.
    pub(crate) fn load(
        peer_assigner: PeerAssigner<S>,
        should_report: bool
    ) -> Self {
        let config = &peer_assigner.conf();
        let node_id = config.id;
        // init router, peers and topo here
        // let mut router = SparseIndex::default();
        let mut partitions = Sparse::default();
        let peers = LocalPeers::default();
        // scan and restore groups to memory. this only includes groups on this node,
        // not full groups of federation.
        let multi_store = peer_assigner.multi_storage.clone();
        let mut new_endpoints = HashSet::new();

        let stopwatch = Instant::now();
        let complete = multi_store.scan_peers(
            Filter::new(), 
            |group| {
            let GroupProto {
                id,
                from_key,
                to_key,
                ..
            } = &group;
            let from_key = Key::left(from_key);
            let to_key = Key::right(to_key);
            let group_id = *id;

            let assignment = peer_assigner
                .assign(group, false)
                .and_then(|(peer, added)| {
                    new_endpoints.extend(added);
                    Ok(LocalPeer {
                        peer: Arc::new(peer),
                    })
                })
                .and_then(|local| {
                    peers.insert(group_id, local);
                    Ok(())
                });

            if assignment.is_ok() {
                let part = Partition::new_and_validate(
                    from_key, to_key, group_id
                );
                if let Err(e) = part {
                    // warn
                    error!("failed to restore group-{:?}, {:?}", group_id, e);
                    return false;
                }
                let p = part.unwrap();
                if !partitions.maybe_add(p) {
                    error!("failed to insert group-{:?} to partitions", group_id);
                    return false;
                }
            }
            true
        });
        crate::info!("total load all {} peers elapsed {}ms", peers.len(), stopwatch.elapsed().as_millis());
        if !complete {
            // TODO
        }
        peer_assigner.establish_connections(new_endpoints);
        let topo = peer_assigner.topo().clone();
        Self {
            node_router: NodeRouter {
                id: node_id,
                partitions: Mvcc::from_timeline(Timeline::from_exists(
                    partitions,
                )),
                peers,
            },
            topo,
            peer_assigner,
            report_list: DashSet::new(),
            should_report
        }
    }

    pub async fn start(&self, scheduler: Arc<Scheduler>) -> Yusult<()> {
        self.coprocessor_driver().start().await?;
        let stopwatch = Instant::now();

        let conf = self.conf();
        let percentage = conf.groups_warmup_percentage();
        let retries = conf.preheat_groups_retries;

        let (success, groups) = self.preheat(percentage, retries).await;
        crate::info!(
            "preheat {} peers success? {success}. total elapsed {}ms", 
            groups, 
            stopwatch.elapsed().as_millis()
        );
        if !success && !conf.preheat_allow_failure {
            crate::error!("failed to preheat multi-groups, decide to abort immediately");
            return Err(YuError::Abort);
        }
        scheduler.start();
        Ok(())
    }

    async fn preheat_once(&self, offset: usize, expected: usize) -> usize {
        let node_num = self.topo().node_num();
        let cur_node = self.id;
        let mut succeed = 0;
        for (i, peer) in self.peers.iter().enumerate() {
            if peer.campaigned().await {
                continue;
            }
            if (i % node_num) as u64 + 1 != cur_node {
                continue;
            }
            if let Ok(role) = peer.election().await {
                succeed += 1;
                if role == RaftRole::Leader {
                    self.add_to_report(peer.get_group_id());
                }
                if offset + succeed >= expected {
                    return succeed;
                }
            }
        }
        succeed
    }

    /// Warmup groups on the node, make part of them initial the election if they have 
    /// not leader before
    async fn preheat(&self, percentage: f32, mut retries: u32) -> (bool, usize) {
        let node_num = self.topo().node_num();
        if node_num < 1 {
            return (false, 0);
        }
        let groups = self.peers.len();
        let expected = if groups <= 7 {
            groups
        } else {
            ((groups / node_num) as f32 * percentage).ceil() as usize
        };
        let mut total_succeed = 0;

        let minimal_wait = std::cmp::max(groups * 3, 3000) as u64;
        let wait_dur = Duration::from_millis(minimal_wait);

        while retries > 0 {
            let succeed = self.preheat_once(total_succeed, expected).await;
            total_succeed += succeed;
            if expected <= total_succeed {
                return (true, total_succeed);
            }
            crate::tokio::time::sleep(wait_dur).await;
            retries -= 1;
        }
        (false, total_succeed)
    }

    #[inline]
    pub(crate) async fn transaction(&self) -> Transaction<S> {
        Transaction::new(&self).await
    }

    /// Assign group on this node in a transaction.
    pub(crate) async fn try_assign_group(&self, group: GroupProto) -> Yusult<()> {
        let mut tx = self.transaction().await;
        let assignment = tx.assign_group(group);
        if let Err(e) = assignment {
            tx.rollback().await;
            Err(e)
        } else {
            tx.commit().await;
            Ok(())
        }
    }
}

impl<S: GroupStorage> NodeManager<S> {

    #[inline]
    pub fn router(&self) -> NodeRouter {
        self.node_router.clone()
    }

    #[inline]
    pub fn conf(&self) -> &NodeConfig {
        &self.peer_assigner.coprocessor.conf()
    }

    #[inline]
    pub fn topo(&self) -> &Topo {
        &self.topo
    }

    #[inline]
    pub fn store(&self) -> &MultiStore<S> {
        &self.peer_assigner.multi_storage
    }

    #[allow(unused)]
    #[inline]
    pub(crate) fn coprocessor_driver(&self) -> &Arc<CoprocessorDriver> {
        &self.peer_assigner.coprocessor
    }

    /// Mark this group should be reported.
    #[inline(always)]
    pub fn add_to_report(&self, group: GroupID) {
        if !self.should_report {
            return;
        }
        self.report_list.insert(group);
    }
}
