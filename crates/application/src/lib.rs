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

#[cfg(all(test, feature = "rpc_transport", feature = "single"))]
mod tests {

    #[test]
    fn helloworld() {
        run();
    }

    use std::io::Result;
    use yu::{
        common::protocol::{read_state::ReadState, NodeID},
        coprocessor::listener::{proposal::RaftListener, Acl, RaftContext},
        peer::{config::NodeConfig, facade::Facade},
        protos::{raft_group_proto::GroupProto, raft_log_proto::Entry},
        solutions::builder::single::{provider, Builder},
        solutions::rpc::StubConfig,
        torrent::topology::node::Node,
        RaftRole,
    };
    use crate as yu;

    /// Step 1: we define a struct called "HelloWorld"
    struct HelloWorld(NodeID);
    impl HelloWorld {
        /// HelloWorld will take out committed entry data
        /// supposed we propose some names to it.
        fn sayhello(&self, ent: &Entry) -> i64 {
            let log = ent.data.clone();
            if let Ok(name) = String::from_utf8(log) {
                println!("[Node {}] Hello {}", self.0, name);
            }
            ent.data.len() as i64
        }
    }

    /// always enable by using default, to control the access privilege of this listener.
    impl Acl for HelloWorld {}

    /// Step 2: implement `RaftListener` for it, so that it can aware the "write" and "read"
    /// events.
    #[crate::async_trait]
    impl RaftListener for HelloWorld {
        /// To handle committed log entry at local.
        async fn handle_write(&self, _: &RaftContext, entries: &[Entry]) -> Result<i64> {
            let mut total = 0;
            for ent in entries.iter() {
                total += self.sayhello(ent);
            }
            Ok(total)
        }

        async fn handle_read(&self, _: &RaftContext, _: &mut ReadState) {
            // ignore this.
        }
    }

    // simulate 3 Node with RPC transport
    fn run() {
        let group = GroupProto {
            id: 1,                                 // group 1
            confstate: Some(vec![1, 2, 3].into()), // has voters [1,2,3]
            // address of these voters
            endpoints: vec![
                Node::parse(1, "raft://localhost:8081").unwrap().into(),
                Node::parse(2, "raft://localhost:8082").unwrap().into(),
                Node::parse(3, "raft://localhost:8083").unwrap().into(),
            ],
            ..Default::default()
        };
        let mut peers = vec![];

        // Step 3: build a "Node" for this raft application.
        for node_id in 1..=3 {
            let mut conf = NodeConfig::default();
            // node id must be assigned
            conf.id = node_id;
            let node = Builder::new(group.clone(), conf)
                .with_raftlog_store(|group| provider::mem_raftlog_store(group)) // save raft's log in memory
                .use_default() // default to tick it and use RPC transport.
                .add_raft_listener(HelloWorld(node_id)) // add this listener to coprocessor
                .build() // ready
                .unwrap();
            // run a daemon to receive RaftMessage between peers in the group.
            let _ = node.start_rpc(StubConfig {
                address: format!("localhost:808{node_id}"), // RPC server's address
                run_as_daemon: true, // run RPC stub in a standalone thread without blocking it
                print_banner: false,
            });
            let peer = node.get_local_client();
            peers.push(peer);
        }
        // try to election and generate a leader for group.
        for peer in peers.iter() {
            if let Ok(RaftRole::Leader) = peer.election() {
                break;
            }
        }
        // Finally, say hello to each others, both leader and follower can
        // handle propose and read_index.
        for (i, name) in ["Alice", "Bob", "Charlie"].iter().enumerate() {
            let proposal = peers[i].propose(name);
            assert!(proposal.is_ok());
        }
    }
}
