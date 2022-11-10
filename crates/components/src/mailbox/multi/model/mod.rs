pub mod indexer;
pub mod check;

use std::{time::SystemTime, fmt::Display};
use common::{protocol::PeerID};

pub struct Location {
    location: PeerID,
    ts: u64,
}

impl Location {

    pub fn new(peer: PeerID) -> Self {
        Self { location: peer, ts: _now() }
    }

    #[inline]
    pub fn elapsed(&self) -> u64 {
        _now() - self.ts
    }

    /// Build `Located` with result, and set elapsed time(in ms).
    pub fn build<T>(self, result: T) -> Located<T> {
        let Location { location, ts } = self;
        let elapsed_ms = _now() - ts;
        Located { location, elapsed_ms, result }
    }
}

fn _now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system clock went backforward")
        .as_millis() as u64
}

#[derive(Debug)]
pub struct Located<T> {
    pub location: PeerID,
    pub elapsed_ms: u64,
    pub result: T,
}

impl<T: Display> Display for Located<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Located { location, elapsed_ms, result } = self;
        let (group, node) = *location;
        write!(f, "Effect on Node-{node} Group-{group}, return: {result} total elapsed {elapsed_ms}ms")
    }
}