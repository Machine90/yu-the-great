use std::{ops::Range, sync::Arc};

use application::{
    multi::{node::Node, store::mem_store::MultiMemStore},
    peer::config::NodeConfig,
    solutions::{builder::multi::Builder, rpc::StubConfig},
    store::memory::store::PeerMemStore,
};

use components::{
    mailbox::{topo::parse_address, RaftEndpoint},
    protos::{
        raft_group_proto::{EndpointProto, GroupProto},
        raft_log_proto::ConfState,
    },
    torrent::partitions::{index::sparse::Sparse, partition::Partition}, vendor::prelude::local_dns_lookup,
};

#[allow(unused)]
use super::kv::{btree_engine::BTreeEngine, hash_engine::HashEngine};

fn nodes(addr: &str) -> Vec<EndpointProto> {
    let mut endpoints = Vec::new();
    parse_address(addr.to_owned(), |id, host, port| {
        endpoints.push(
            RaftEndpoint::parse(id, format!("raft://{host}:{port}").as_str())
                .unwrap()
                .into(),
        );
    });
    endpoints
}

fn _mock_rang_groups(template: GroupProto, range: Range<u32>) -> Vec<GroupProto> {
    let mut groups = Vec::new();
    let mut partitions = Sparse::infinity(1);
    for i in range {
        let split = partitions.estimate_split(format!("{i}"));
        if let Some((s, e)) = split {
            partitions.maybe_add(Partition::from_range(s.as_left()..e.as_right(), i));
        }
    }
    for (_, p) in partitions.iter_mut() {
        let mut group = template.clone();
        group.id = p.resident;
        group.set_key_range(p.from_key.as_left()..p.to_key.as_right());
        groups.push(group);
    }
    groups
}

fn _mock_specific_groups(template: GroupProto, keys: Vec<&'static str>) -> Vec<GroupProto> {
    let end = keys.len() - 1;
    let mut groups = Vec::new();
    for group_id in 1..=end {
        let mut group = template.clone();
        group.id = group_id as u32;
        let from = keys[(group_id - 1) as usize];
        let to = keys[(group_id) as usize];
        group.set_key_range(from..to);
        groups.push(group);
    }
    groups
}

pub fn mock_node() -> Vec<GroupProto> {
    let template = GroupProto {
        id: 1,
        from_key: Vec::new(), // min
        to_key: Vec::new(),   // max
        confstate: Some(ConfState {
            voters: vec![1, 2, 3],
            ..Default::default()
        }),
        endpoints: nodes("1-127.0.0.1:20071,2-127.0.0.1:20072,3-127.0.0.1:20073"),
        ..Default::default()
    };
    _mock_rang_groups(template, 1..8)
}

pub fn build_node(id: u64, addr: &str, empty: bool) -> Node<PeerMemStore> {
    let mut config = NodeConfig::default();
    config.id = id;
    config.preheat_groups_percentage = 0.7;
    config.preheat_groups_retries = 8;
    config.preheat_allow_failure = true;
    let socket = local_dns_lookup(addr).unwrap();
    config.host = socket.ip().to_string();
    config.mailbox_port = socket.port();
    
    let nb = Builder::new(config);

    let store = if !empty { 
        MultiMemStore::restore_from(mock_node())
    } else {
        MultiMemStore::new()
    };

    let kv_db = Arc::new(HashEngine::new());
    let nb = nb
        .with_raftlog_store(store)
        .add_raft_listener_ref(kv_db.clone())
        .register_balancer_ref(kv_db.clone())
        .use_default();
    let n = nb.build();

    let _r = n.start_rpc(StubConfig {
        address: addr.to_owned(),
        print_banner: false,
        run_as_daemon: true,
    });
    n
}
