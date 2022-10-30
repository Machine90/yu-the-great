use application::multi::node::Node;
use application::store::memory::store::PeerMemStore;
use components::common::protocol::NodeID;
use components::common::protocol::read_state::ReadState;
use components::mailbox::multi::api::MultiRaftApi;
use components::mailbox::multi::model::Located;
use components::protos::multi_proto::Assignments;
use components::protos::multi_proto::Merge;
use components::protos::multi_proto::Split;
use components::protos::multi_proto::Transfer;
use components::tokio1::runtime::Runtime;
use components::vendor::prelude::DashMap;
use components::vendor::prelude::LogLevel;
use multi_db::node_three;
use multi_db::set_logger;
use std::ops::Range;
use std::sync::Arc;

use crate::multi_db::kv::Operation;

mod multi_db;

const INITIALIZED: &str = r#"
Initialized a multi-raft cluster with 3 nodes [1,2,3]
Play it and enter some commands:
"#;
const HELP: &str = r#"
add node address 
    desc: add a node to cluster
    example `add 1 127.0.0.1:8080`
stat node group
    desc: print group status of node one of [1,2,3]
    example: `stat 1 1`
part node
    desc: print partitions of node one of [1,2,3]
    example: `part 1`
topo node
    desc: print topology of node one of [1,2,3]
    example: `topo 1`
conn node
    desc: print connections of node one of [1,2,3]
    example: `conn 1`
put key value
    desc: put the key and value to multi-raft store.
    example: `put test_1 helloworld`
get key
    desc: get the key from multi-raft store.
    example: `get test_1`, then print helloworld
split group split_key_1
    desc: split specific group with key
    example: `split 1 c`, split group 1 in range ["a", "d") with key "c" into ["a", "c") ["c", "d")
merge groups
    desc: request for merging target groups into a large one.
    example: `merge 2,3,4,5` make sure the group is adjance.
help
    desc: print this message
    example: `help`
"#;

fn main() {
    set_logger(LogLevel::Debug, true);
    Runtime::new().unwrap().block_on(three());
}

type MemNode =  Node<PeerMemStore>;
#[derive(Default)]
struct SimulateCluster {
    nodes: DashMap<NodeID, Arc<MemNode>>,
}

impl SimulateCluster {
    fn add(&self, node: MemNode) {
        self.nodes.insert(node.id, Arc::new(node));
    }

    fn get(&self, node: u64) -> Option<Arc<MemNode>> {
        self.nodes.get(&node)
            .map(|v| v.value().clone())
    }

    fn list(&self) {
        println!("+---------------------------------------------------------------------+");
        for node in self.nodes.iter() {
            let id = node.id;
            let node = node.conf().get_endpoint();
            if let Ok(node) = node {
                println!("| node: {id} => {:?} |", node);
            }
        }
        println!("+---------------------------------------------------------------------+")
    }
}

#[allow(unused)]
async fn three() {
    let cluster = Arc::new(SimulateCluster::default());
    for id in 1..=3 {
        let n = node_three::build_node(
            id, 
            format!("127.0.0.1:2007{id}").as_str(), 
            false
        );
        cluster.add(n);
    }
    // node 1 group 1
    println!("{INITIALIZED}{HELP}");

    loop {
        println!("[>>> enter command: >>]:");
        let mut cmd = String::default();
        if std::io::stdin().read_line(&mut cmd).is_err() {
            continue;
        }
        resolve_cmd(cmd, cluster.clone()).await;
    }
}

async fn resolve_cmd(cmd: String, nodes: Arc<SimulateCluster>) -> Option<()> {
    if cmd.starts_with("topo") {
        // topo 1 => print topology of node 1
        let node = node_idx(cmd.as_str(), 4);
        println!("{:?}", nodes.get(node)?.topo().get_topo());
    } else if cmd.starts_with("conn") {
        // conn 1 => print connections of node 1
        let node = node_idx(cmd.as_str(), 4);
        println!("{:#?}", nodes.get(node)?.topo().get_conns());
    } else if cmd.starts_with("part") {
        let node = node_idx(cmd.as_str(), 4);
        println!("+--------------- Partitions of Node {node} ------------------+");
        for (cnt, (_, p)) in nodes.get(node)?.partitions().current().iter().enumerate() {
            println!("| key range: {p} => in group-{}", p.resident);
            if cnt > 16 {
                break;
            }
        }
        println!("+-------------------------------------------------------+");
    } else if cmd.starts_with("stat") {
        let node = find_num(cmd.as_str(), 4..6)? as u64;
        let group = find_num(cmd.as_str(), 6..8)?;
        let peer = nodes.get(node)?.find_peer(group as u32).ok()?;
        println!("{:#?}", peer.status(true).await);
    } else if cmd.starts_with("put") {
        let tokens: Vec<_> = cmd.split(" ").collect();
        let key = trim(tokens.get(1)?);
        let value = trim(tokens.get(2)?);
        let proposal = nodes.get(1)?.propose(Operation::put(key, value)).await.ok()?;
        println!("{:?}", proposal);
    } else if cmd.starts_with("get") {
        let tokens: Vec<_> = cmd.split(" ").collect();
        let key = trim(tokens.get(1)?);
        let rd = nodes.get(1)?
            .read_index(Operation::get(key)).await.ok()?;
        print_readed(rd);
    } else if cmd.starts_with("help") {
        println!("{HELP}");
    } else if cmd.starts_with("cluster") {
        nodes.list();
    } else if cmd.starts_with("merge") {
        let tokens: Vec<_> = cmd.split(" ").collect();
        let target = trim(tokens.get(1)?);
        let src_groups: Vec<u32> = target.split(',').map(|n| {
            trim(n).parse::<u32>().unwrap_or_default()
        }).filter(|n| *n != 0).collect();
        let deleg = nodes.get(1)?;

        // TODO: compactions is incorrect now.
        deleg.coordinator().as_ref()?.process_assignments(Assignments {
            should_merge: vec![
                Merge { src_groups, ..Default::default() }
            ],
            ..Default::default()
        }).await;
    } else if cmd.starts_with("split") {
        // split 1 "split_key_1"
        let tokens: Vec<_> = cmd.split(" ").collect();
        let src_group = trim(tokens.get(1)?).parse::<u32>().ok()?;
        let split_key = trim(tokens.get(2)?).as_bytes().to_vec();
        let deleg = nodes.get(1)?;
        let next_group = deleg.partitions()
            .current()
            .iter().map(|(_, p)| p.resident)
            .max()?;

        deleg.coordinator().as_ref()?.process_assignments(Assignments {
            should_split: vec![Split { 
                src_group, 
                dest_group: next_group + 1, 
                config_split_key: split_key
            }],
            ..Default::default()
        }).await;
    } else if cmd.starts_with("trans") {
        // cmd   group from to
        // trans 1     1    4
        let tokens: Vec<_> = cmd.split(" ").collect();
        let group = trim(tokens.get(1)?).parse::<u32>().ok()?;
        let src = trim(tokens.get(2)?).parse::<u64>().ok()?;
        let dest = trim(tokens.get(3)?).parse::<u64>().ok()?;

        let node = nodes.get(src)?;
        let target = nodes.get(dest)?.conf().get_endpoint().ok()?.into();
        // unstable now
        node.coordinator().as_ref()?.process_assignments(Assignments {
            should_transfer: vec![Transfer { group, transfee: Some(target) }],
            ..Default::default()
        }).await;

    } else if cmd.starts_with("add") {
        // `add 1 localhost:8080`
        let tokens: Vec<_> = cmd.split(" ").collect();
        let id = trim(tokens.get(1)?).parse::<u64>().ok()?;
        let addr = trim(tokens.get(2)?);
        nodes.add(node_three::build_node(
            id, 
            addr.as_str(), 
            true
        ));
    }
    None
}

fn node_idx(cmd: &str, prefix_bits: usize) -> u64 {
    let node = if cmd.len() > prefix_bits {
        cmd[prefix_bits..]
            .trim()
            .parse::<u64>()
            .ok()
            .unwrap_or(0)
    } else {
        0
    };
    node
}

fn find_num(cmd: &str, range: Range<usize>) -> Option<usize> {
    let len = cmd.len();
    if len < range.end {
        return None;
    }
    cmd[range].trim().parse::<usize>().ok()
}

#[inline]
fn trim(v: &str) -> String {
    v.trim().replace("\n", "")
}

fn print_readed(result: Located<ReadState>) {
    let Located {
        location: (group, node),
        elapsed_ms,
        result: ReadState { index, request_ctx },
    } = result;

    let result = format!("value {:?}, index: {index}", String::from_utf8(request_ctx));
    println!("Data on the node-{node} group-{group}\nresult: {result}\nelapsed: {elapsed_ms}ms");
}
