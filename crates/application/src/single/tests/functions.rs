use crate::{
    self as yu,
    peer::facade::{local::LocalPeer, AbstractPeer},
    single,
};
use common::vendor::prelude::{
    conf_file_logger, init_logger_factory, LogFactory, LogLevel, LoggerConfig,
};
use components::{
    tokio1::time::{sleep, Instant},
    torrent::runtime,
    utils::endpoint_change::{ChangeSet, EndpointChange},
};
use consensus::HashMap;
use std::{io::Result, time::Duration};
use yu::{
    common::protocol::{read_state::ReadState, NodeID},
    coprocessor::listener::{proposal::RaftListener, Acl, RaftContext},
    peer::{config::NodeConfig, facade::Facade},
    protos::raft_group_proto::GroupProto,
    solutions::builder::single::{provider, Builder},
    solutions::rpc::StubConfig,
    torrent::topology::node::Node,
    RaftRole,
};

use super::set_logger;

/// Propose "Alice", "Bob", "Chris", "David", "Elias", "Frank"  to `HelloWorld`
/// in parallel, these propose will be appended and applied in the same order.
#[test]
fn test_helloworld() {
    set_logger("hello_world");
    runtime::blocking(hello_world());
}

#[test]
fn test_confchange() {
    set_logger("conf_change");
    runtime::blocking( conf_change());
}

/// Step 1: we define a struct called "HelloWorld"
struct HelloWorld;

/// always enable by using default, to control the access privilege of this listener.
impl Acl for HelloWorld {}

/// Step 2: implement `RaftListener` for it, so that it can aware the "write" and "read"
/// events.
#[crate::async_trait]
impl RaftListener for HelloWorld {
    /// To handle committed log entry at local.
    async fn handle_write(&self, ctx: &RaftContext, data: &[u8]) -> Result<i64> {
        let RaftContext { node_id, .. } = ctx;
        if let Ok(name) = String::from_utf8(data.to_vec()) {
            println!("[App] Node {}: Hello {}", node_id, name);
        }
        Ok(data.len() as i64)
    }

    async fn handle_read(&self, _: &RaftContext, _: &mut ReadState) {
        // ignore this.
    }
}

fn _build_node(node_id: NodeID, group: GroupProto) -> single::Node {
    let mut conf = NodeConfig::default();
    // node id must be assigned
    conf.id = node_id;
    conf.enable_pre_vote_round = true;
    let node = Builder::new(group.clone(), conf)
        .with_raftlog_store(|group| provider::mem_raftlog_store(group)) // save raft's log in memory
        .use_default() // default to tick it and use RPC transport.
        .add_raft_listener(HelloWorld) // add this listener to coprocessor
        .build() // ready
        .unwrap();
    // run a daemon to receive RaftMessage between peers in the group.
    let _ = node.start_rpc(StubConfig {
        address: format!("localhost:808{node_id}"), // RPC server's address
        run_as_daemon: true, // run RPC stub in a standalone thread without blocking it
        print_banner: false,
    });
    node
}

fn three_peers() -> (
    HashMap<NodeID, single::Node>,
    GroupProto,
    Vec<LocalPeer>,
    Option<LocalPeer>,
) {
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

    let mut nodes = HashMap::default();
    // Step 3: build a "Node" for this raft application.
    for node_id in 1..=3 {
        let node = _build_node(node_id, group.clone());
        let peer = node.get_local_client();
        nodes.insert(node.node_id(), node);
        peers.push(peer);
    }

    let mut leader = None;
    // try to election and generate a leader for group.
    for peer in peers.iter() {
        if let Ok(RaftRole::Leader) = peer.election() {
            leader = Some(peer.clone());
            break;
        }
    }
    (nodes, group, peers, leader)
}

// simulate 3 Node with RPC transport
async fn hello_world() {
    let (_, _, peers, leader) = three_peers();

    // Finally, say hello to each others, both leader and follower can
    // handle propose and read_index.
    let mut ts = vec![];

    for (i, name) in ["Alice", "Bob", "Chris", "David", "Elias", "Frank"]
        .iter()
        .enumerate()
    {
        let idx = i % 3;
        let p = peers[idx].clone();
        let t = runtime::spawn(async move {
            let id = idx + 1;
            println!("[App] Node {id}: start propose");
            let timer = Instant::now();
            let proposal = p.propose_async(name.as_bytes().to_vec()).await;
            println!(
                "[App] Node {id}: result {:?}, elapsed: {:?}ms",
                proposal,
                timer.elapsed().as_millis()
            );
            assert!(proposal.is_ok());

            let r = p.read_async(name.as_bytes().to_vec()).await;
            if let Ok(r) = r {
                let ReadState { index, request_ctx } = r;
                println!(
                    "[App] Node {id}: read {index} {:?}",
                    String::from_utf8(request_ctx)
                );
            } else {
                eprintln!("{:?}", r.err());
            }
        });
        ts.push(t);
    }
    for t in ts {
        let _ = t.await;
    }
    let s = leader.unwrap().status(true).await;
    println!("{:#?}", s);
}

/// Test `conf_change` for single raft group, initial group with nodes `[1, 2, 3]` 
/// add node 4 and remove node 2. Then transfer leader to peer 4.
async fn conf_change() {
    let (mut nodes, mut group, _, leader) = three_peers();
    let leader = leader.unwrap();
    let _ = leader.propose_async(b"Zhang 3".to_vec()).await;
    let _ = leader.propose_async(b"Li 4".to_vec()).await;
    let _ = leader.propose_async(b"Wang 5".to_vec()).await;

    let endpoint4 = Node::parse(4, "raft://localhost:8084").unwrap();
    group.add_voter(&endpoint4);
    let peer4 = _build_node(4, group).get_local_client();
    let to_remove = 2;
    // add 4 and remove 2
    let changes = ChangeSet::new()
        .add(EndpointChange::add_as_voter(&endpoint4))
        .add(EndpointChange::remove(to_remove));
    let stat = leader
        .propose_conf_changes_async(changes.into(), true)
        .await;
    if let Ok(stat) = stat {
        let mut cs = stat.unwrap().conf_state.unwrap();
        if !cs.voters_outgoing.is_empty() {
            cs.voters_outgoing.sort();
            println!("{:?}", cs.voters_outgoing);
            assert_eq!(cs.voters_outgoing, vec![1,2,3]);
        }
        cs.voters.sort();
        assert_eq!(cs.voters, vec![1,3,4]);
    }

    if let Some(node2) = nodes.remove(&to_remove) {
        // stop the removed server, in case that it publish an election, or
        // enable prevote to guarantee the quorum.
        let _ = node2.stop().await;
    }
    sleep(Duration::from_millis(100)).await;
    let _ = leader.propose_async(b"Zhu 6".to_vec()).await;
    let _ = peer4.propose_async(b"Xie 7".to_vec()).await;
    let stat = leader.status(true).await;
    // after removing 2 and adding 4, the conf_state should end up with voters [1,3,4]
    let mut cs = stat.conf_state.unwrap();
    cs.voters.sort();
    assert_eq!(cs.voters, vec![1,3,4]);
    let _ = peer4.transfer_leader_async(4).await;
    // transfer leader guarantee the target must be Leader after transfered
    assert_eq!(peer4.role().await, RaftRole::Leader);
}
