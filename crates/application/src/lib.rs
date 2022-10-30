pub mod coprocessor;
pub mod store;
pub mod peer;
pub mod engine;
pub mod solutions;

#[cfg(feature = "multi")] pub mod multi;
#[cfg(feature = "single")] pub mod single;

// For public use
pub use components::async_trait::async_trait;
pub use consensus::raft::{SoftState, raft_role::RaftRole};
#[cfg(feature = "rpc_transport")] pub use transport;

// For internal use
use common;
use common::protocol::{GroupID, NodeID, PeerID};
use common::protos;
use common::errors::{Error as ConsensusError, Result as RaftResult, application::{Yusult, YuError}};
use common::protos::{
    raft_payload_proto::{
        Message as RaftMsg, 
        MessageType as RaftMsgType
    }
};

use components;
use components::mailbox;
use components::storage;
use components::vendor;
use components::torrent;

use consensus::{config::Config as RaftConfig};
use consensus::raft_node::status::Status as RaftStatus;

#[allow(unused)]
use vendor::prelude::{error, warn, debug, info, trace};
use components::tokio1 as tokio;

pub mod utils {
    pub fn drain_filter<T, R>(items: &mut Vec<T>, filter: fn(&T) -> bool, reduce: fn(T) -> Option<R>) -> Vec<R> {
        let mut another = Vec::new();
        let mut i = 0;
        while i != items.len() {
            let item = &items[i];
            if filter(item) {
                let prev = items.remove(i);
                let r = reduce(prev);
                if r.is_none() { continue; }
                another.push(r.unwrap());
            } else {
                i += 1;
            }
        }
        another
    }
}
