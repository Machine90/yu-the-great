<!-- <img src="./document/imgs/logo.png" align="left" width="200"/> -->

## [Documentation](document/) | 

**Yu The Great** is an open-source distributed consensus library which implemented by Rust based on the raft algorithm, can help developers to write some distributed products easily, for example distributed key-value store, distributed message queue and some tools like distributed lock that can be embed in your project.

Unlike others distributed project, Yu is not a standalone server program, so that developers don't need to deploy it separately, just integrate it with your project. Yu is inspired by some great distributed systems from HBase, TiKV and etcd, is aimed to make it easily to develop scalable distributed project.

About name, Yu the great, also known as  Da yu, hero of China in ancient times who tamed the floods and protected residents from disaster, he drained and dredged the water and diverted the flood into the sea...  We hope this project can help others to tame the network data flow same as Yu who tamed the floods.

## Quick Start

### Concepts

Let's introduce some important concepts before using this library:

* Node: the Node we mentioned here could be a server, a node can maintain more than 1 group.
* Group: a raft group, includes some voters (consists of different nodes), each group has it's own id, key range and voters.
* Peer: a member (aka voter) of group on this node, perform as a `RaftNode`
* Propose: "write" behavior of the raft group, content of propose is defined to binary type, which can be designed to anything, for example "binlog".
* ReadIndex: "read" behavior of the raft group, 

### Dependency

We haven't deploy it to any repo like crate.io (in plan) now, so if you have interesting, import it from git.

```
yu-the-greate = { git = "https://github.com/xxx.git", default-features = false }
```

### Features

* **Default feature**: "application", which implement basic process of Raft, with some core modules: 

  * Raft's log memory store implementation.

  * Process: includes the basic process of raft, based on raft-rs implementation.

  * Coprocessor: help to handle raft's processes, for example "commit", "read_index", state changes etc, then send all these events to "Listener".

  * Listener: we define several listeners, a listener is the basic unit of architecture which used to aware changes and handle "read", "write" event, and  also help to send snapshot.    

  * Scheduler, which used to do some chore jobs, for example tick raft group.

  * Components, include the trait definition of:

    * Mailbox: used to transfer messages between voters in each group. 
    * Storage: used to persist raft log.

    the implementations of these traits could be developed in another project.

* **single**: we provide a single raft group solution in this feature, include:

  * A schedule used to tick single raft group period.
  *  Builder for single raft group, help to create Node quickly.

* **multi**: We plan to support "Multi-Raft" on this library, but still in developing now.

  * NodeManager: 
  * Coordinator: 
  * BatchTicker: 

* **rpc**: RPC implementation of mailbox, powered by Tarpc

### Code samples

#### "single" & "rpc"

```rust
// features = ["single", "rpc"]
use std::{io::Result};
use yu_boot::yu_the_great as yu;
use yu::{
    common::protocol::{read_state::ReadState, NodeID},
    coprocessor::listener::{proposal::RaftListener, Acl, RaftContext},
    peer::{config::NodeConfig, facade::Facade},
    protos::{raft_group_proto::GroupProto, raft_log_proto::Entry},
    solutions::builder::single::{provider, Builder},
    solutions::rpc::StubConfig,
    torrent::{runtime, topology::node::Node},
    RaftRole,
};

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
#[yu::async_trait]
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

fn main() {
    let group = GroupProto {
        id: 1, // group 1
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
            .with_storage(|group| provider::mem_store(group)) // save raft's log in memory
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
        if let Ok(RaftRole::Leader) = runtime::blocking(peer.election()) {
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
```

#### "multi" & "rpc"

TODO

#### More Example

TODO (link to examples)



