use std::sync::Arc;

use common::metrics::sys_metrics::SysMetrics;
use components::{storage::group_storage::GroupStorage, vendor::debug};

use crate::{
    engine::sched::schedule::ScheduleAsync,
    multi::node::coordinator::NodeCoordinator,
    tokio::sync::Mutex,
    tokio::{sync::RwLock, time::Instant},
};

pub struct GroupChecker<S: GroupStorage> {
    coordinator: Arc<NodeCoordinator<S>>,
    last_time: Arc<RwLock<Instant>>,
    mutex: Arc<Mutex<()>>,
}

impl<S: GroupStorage> GroupChecker<S> {
    pub fn new(coordinator: Arc<NodeCoordinator<S>>) -> Self {
        Self {
            coordinator,
            last_time: Arc::new(RwLock::new(Instant::now())),
            mutex: Arc::new(Mutex::new(())),
        }
    }

    fn _evaluate_overload(metrics: &SysMetrics) -> bool {
        let (one, five, fifteen) = metrics.cpu_load;
        let core = metrics.cpu_physical_cores as f64;
        if one > (0.7 * core) || five > (0.6  * core) || fifteen > (0.5  * core) {
            return true;
        }

        if metrics.cpu_usage >= 70. {
            return true;
        }
        false
    }
}

#[crate::async_trait]
impl<S: GroupStorage> ScheduleAsync for GroupChecker<S> {
    #[inline]
    fn token(&self) -> &'static str {
        "GroupChecker"
    }

    async fn nocked(&self, _: u64) -> bool {
        let now = Instant::now();
        let dur = now.duration_since(*self.last_time.read().await);

        let conf = self.coordinator.balancer.conf();
        if dur >= conf.check_interval() {
            *self.last_time.write().await = now;
            true
        } else {
            false
        }
    }

    #[inline]
    async fn fire(&self, _: u64) {
        if let Ok(busy) = self.mutex.try_lock() {
            let metrics = self.coordinator.sampling_sys(true);
            if let Some(metrics) = metrics {
                if Self::_evaluate_overload(&metrics) {
                    drop(busy);
                    debug!("CPU overload now, try sampling groups next time, see:\n{metrics}");
                    return;
                }
            }
            self.coordinator.sampling_groups(true).await;
            drop(busy);
        }
    }
}
