use std::sync::Arc;
use crate::{engine::sched::schedule::{ScheduleAsync}, peer::Peer, warn};

pub struct SingleTicker {
    peer: Arc<Peer>
}

impl SingleTicker {
    pub fn new(peer: Arc<Peer>) -> Self {
        Self { peer }
    }
}

#[crate::async_trait]
impl ScheduleAsync for SingleTicker {
    fn token(&self) -> &'static str {
        "SingleTicker"
    }

    async fn nocked(&self, _tick: u64) -> bool {
        true
    }

    async fn fire(&self, _tick: u64) {
        let t = self.peer.tick(true, true).await;
        if let Err(e) = t {
            warn!("failed to tick peer, see: {:?}", e);
        }
    }
}
