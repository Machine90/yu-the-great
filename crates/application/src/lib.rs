pub mod coprocessor;
pub mod engine;
pub mod peer;
pub mod solutions;
pub mod store;

#[cfg(feature = "multi")]
pub mod multi;
#[cfg(feature = "single")]
pub mod single;

// For public use
pub use components::async_trait::async_trait;
pub use consensus::raft::{raft_role::RaftRole, SoftState};
#[cfg(feature = "rpc_transport")]
pub use transport;

// For internal use
use common;
use common::errors::{
    application::{YuError, Yusult},
    Error as ConsensusError, Result as RaftResult,
};
use common::protocol::{GroupID, NodeID, PeerID};
use common::protos;
use common::protos::raft_payload_proto::{Message as RaftMsg, MessageType as RaftMsgType};

use components;
use components::mailbox;
use components::storage;
use components::torrent;
use components::vendor;

use consensus::config::Config as RaftConfig;
use consensus::raft_node::status::Status as RaftStatus;

use components::tokio1 as tokio;
#[allow(unused)]
use vendor::prelude::{debug, error, info, trace, warn};

pub mod utils {
    pub fn drain_filter<T, R>(
        items: &mut Vec<T>,
        filter: fn(&T) -> bool,
        reduce: fn(T) -> Option<R>,
    ) -> Vec<R> {
        let mut another = Vec::new();
        let mut i = 0;
        while i != items.len() {
            let item = &items[i];
            if filter(item) {
                let prev = items.remove(i);
                let r = reduce(prev);
                if r.is_none() {
                    continue;
                }
                another.push(r.unwrap());
            } else {
                i += 1;
            }
        }
        another
    }
}

