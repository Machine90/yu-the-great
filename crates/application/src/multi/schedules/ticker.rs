use std::{cmp, collections::HashMap, ops::Deref, sync::Arc, time::Duration};

use crate::{
    multi::node::manager::NodeManager,
    peer::process::tick::Ticked,
    tokio::{
        sync::{Mutex, RwLock},
        time::Instant,
    },
};
use common::{
    protocol::NodeID,
    protos::multi_proto::{BatchMessages, GroupMessage},
};
use components::storage::group_storage::GroupStorage;
use consensus::prelude::RaftRole;

use crate::vendor::prelude::*;
use crate::{
    engine::sched::schedule::ScheduleAsync, multi::node::LocalPeers, peer::process::tick::Kinds,
};
use crate::{peer::facade::local::LocalPeer, torrent::runtime};

pub struct BatchTicker<S: GroupStorage> {
    pub mutex: Arc<Mutex<()>>,
    last_time: Arc<RwLock<Instant>>,
    procedure: Arc<Procedure<S>>,
}

impl<S: GroupStorage> Deref for BatchTicker<S> {
    type Target = Procedure<S>;

    fn deref(&self) -> &Self::Target {
        self.procedure.as_ref()
    }
}

pub struct Procedure<S: GroupStorage> {
    pub node: NodeID,
    cpu_cores: usize,
    node_manager: Arc<NodeManager<S>>
}

impl<S: GroupStorage> Procedure<S> {
    #[allow(unused)]
    #[inline]
    async fn need_tick(&self, peer: &LocalPeer) -> bool {
        self.node_manager
            .coprocessor_driver()
            .monitor()
            .map(|dam| {
                // TODO qps shoud be in a safe range.
                true
            })
            .unwrap_or(true)
    }

    #[inline]
    async fn _tick(&self, batched: Vec<LocalPeer>) {
        if batched.is_empty() {
            return;
        }
        let mut distinct_hbs = HashMap::new();
        // all peers in this node have the same topo and underlying socket connections.
        let mailbox = batched[0].mailbox.clone();
        for peer in batched {
            if !self.need_tick(&peer).await {
                continue;
            }
            let group = peer.get_group_id();
            let ticked = peer.tick(false, true).await;
            if let Err(e) = ticked {
                debug!("tick group {:?} failed: {:?}", group, e);
                continue;
            }
            let Ticked { role, kind } = ticked.unwrap();
            match kind {
                Kinds::PrepareHeartbeat(hbs) => {
                    for hb in hbs {
                        distinct_hbs
                            .entry(hb.to)
                            .or_insert(Vec::new())
                            .push(GroupMessage {
                                group,
                                message: Some(hb),
                            });
                    }
                }
                Kinds::Heartbeat(_no_acks) => {}
                Kinds::Election => {
                    if role == RaftRole::Leader {
                        self.node_manager.add_to_report(group);
                    }
                }
                _ => (),
            }
        }

        for (node, hbs) in distinct_hbs {
            let batched = BatchMessages {
                to: node,
                messages: hbs,
            };
            let _r = mailbox.batch_heartbeats(batched).await;
        }
    }
}

impl<S: GroupStorage> BatchTicker<S> {
    pub fn new(node: NodeID, node_manager: Arc<NodeManager<S>>) -> Self {
        let cpu_cores = num_cpus::get();
        Self {
            mutex: Arc::new(Mutex::new(())),
            last_time: Arc::new(RwLock::new(Instant::now())),
            procedure: Arc::new(Procedure {
                node,
                node_manager,
                cpu_cores
            }),
        }
    }

    #[inline(always)]
    fn peers_num(&self) -> usize {
        self.node_manager.peers.len()
    }

    #[inline(always)]
    fn peers(&self) -> &LocalPeers {
        &self.node_manager.peers
    }

    #[inline]
    fn batch_num(&self) -> (usize, bool) {
        let peer_num = self.peers_num();
        if peer_num > 256 {
            (
                (peer_num as f64 / self.cpu_cores as f64).ceil() as usize,
                true,
            )
        } else {
            (peer_num, false)
        }
    }

    /// depends on how much groups now, for example 1w nodes, then
    /// schedule each 20s, should not less than 500ms.
    #[inline(always)]
    fn _evaluate_tick_duration(&self) -> Duration {
        Duration::from_millis(cmp::max((self.peers_num() * 2) as u64, 500))
    }
}

#[crate::async_trait]
impl<S: GroupStorage> ScheduleAsync for BatchTicker<S> {
    fn token(&self) -> &'static str {
        "BatchTicker"
    }

    async fn nocked(&self, _tick: u64) -> bool {
        let now = Instant::now();
        let dur = now.duration_since(*self.last_time.read().await);
        let expired = if dur >= self._evaluate_tick_duration() {
            *self.last_time.write().await = now;
            true
        } else {
            false
        };
        expired
    }

    async fn fire(&self, _tick: u64) {
        let lock = self.mutex.try_lock();
        if lock.is_err() {
            return;
        }

        let mut tasks = vec![];
        let (batch, should_batch) = self.batch_num();
        let mut collect = Vec::with_capacity(batch);

        let timer = crate::tokio::time::Instant::now();
        if !should_batch {
            for peer in self.peers().iter() {
                let group = peer.value().clone();
                collect.push(group);
            }
            self._tick(collect).await;
        } else {
            for peer in self.peers().iter() {
                let group = peer.value().clone();
                collect.push(group);
                if collect.len() >= batch {
                    let batched = std::mem::replace(&mut collect, Vec::with_capacity(batch));
                    let ticker = self.procedure.clone();
                    let task = runtime::spawn(async move {
                        ticker._tick(batched).await;
                    });
                    tasks.push(task);
                }
            }

            if !collect.is_empty() {
                let ticker = self.procedure.clone();
                let task = runtime::spawn(async move {
                    ticker._tick(collect).await;
                });
                tasks.push(task);
            }

            for task in tasks {
                let j = task.await;
                if let Err(e) = j {
                    warn!("attempt to join BatchTicker, but failed {:?}", e);
                }
            }
        }
        drop(lock);
        crate::trace!(
            "tick multi-groups({}) success, elapsed {:?}ms",
            self.peers_num(),
            timer.elapsed().as_millis()
        );
    }
}
