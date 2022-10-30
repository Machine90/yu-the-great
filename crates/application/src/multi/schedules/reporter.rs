use components::storage::group_storage::GroupStorage;
use std::{sync::Arc, time::Duration};

use crate::{
    engine::sched::schedule::ScheduleAsync,
    multi::node::coordinator::NodeCoordinator,
    tokio::{
        sync::{Mutex, RwLock},
        time::Instant,
    },
};

pub struct NodeReporter<S: GroupStorage> {
    coordinator: Arc<NodeCoordinator<S>>,
    last_time: Arc<RwLock<Instant>>,
    mutex: Arc<Mutex<()>>,
}

impl<S: GroupStorage> NodeReporter<S> {
    pub fn new(coordinator: Arc<NodeCoordinator<S>>) -> Self {
        Self {
            coordinator,
            last_time: Arc::new(RwLock::new(Instant::now())),
            mutex: Arc::new(Mutex::new(())),
        }
    }
}

#[crate::async_trait]
impl<S: GroupStorage + Clone> ScheduleAsync for NodeReporter<S> {
    #[inline]
    fn token(&self) -> &'static str {
        "NodeReporter"
    }

    async fn nocked(&self, _: u64) -> bool {
        let now = Instant::now();
        let dur = now.duration_since(*self.last_time.read().await);

        // TODO: replace with configured value.
        if dur >= Duration::from_millis(10000) {
            *self.last_time.write().await = now;
            true
        } else {
            false
        }
    }

    #[inline]
    async fn fire(&self, _: u64) {
        if let Ok(busy) = self.mutex.try_lock() {
            self.coordinator.report(true).await;
            drop(busy);
        }
    }
}
