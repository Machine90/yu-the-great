use std::{collections::HashSet, convert::TryInto, ops::Deref, str::FromStr, sync::Arc, time::Duration};

use common::protos::{prelude::raft_group_ext::valid_str_list, raft_group_proto::GroupProto};
use torrent::{
    network::Network,
    topology::{node::Node, topo::Topology},
};
use crate::vendor::prelude::*;

use super::{COORDINATOR_GROUP_ID, LABEL_MAILBOX};

/// Using for locating the peer's address in multi-raft
use common::protocol::NodeID;
use common::protocol::GroupID;
pub use common::protocol::PeerID;
pub type Host = String;
pub type Port = u16;

#[derive(Debug, Clone, Copy)]
pub struct TopoConf {
    pub connect_timeout_millis: u64,
}

impl TopoConf {
    #[inline]
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_millis)
    }
}

impl Default for TopoConf {
    fn default() -> Self {
        Self { 
            connect_timeout_millis: 200 
        }
    }
}

impl Deref for Topo {
    type Target = Network<GroupID, NodeID>;

    fn deref(&self) -> &Self::Target {
        self.network.as_ref()
    }
}

impl Default for Topo {
    fn default() -> Self {
        Self {
            conf: Arc::new(TopoConf::default()),
            network: Arc::new(Network::new()),
        }
    }
}

/// `Topo` topology maintain both physical connections of the nodes and
/// endpoints information, all these nodes could be indexed by
/// groupid and nodeid from `Topo`.
///
/// The Topo is thread-safety, just clone it without any concerns,
/// all cloned of Topo are referring to the same cluster reference.
#[derive(Clone)]
pub struct Topo {
    pub conf: Arc<TopoConf>,
    network: Arc<Network<GroupID, NodeID>>,
}

/// example: 1-[1-127.0.0.1:8080,2-127.0.0.1:8081];2-(1-127.0.0.1:8083,2-127.0.0.1:8084);
impl FromStr for Topo {
    type Err = std::io::Error;

    fn from_str(groups: &str) -> Result<Self, Self::Err> {
        let topo = Topology::new();
        parse_group_address(groups.to_owned(), |group, node, host, port| {
            if group == COORDINATOR_GROUP_ID {
                warn!("should not set `coordinator` group with id 0 in topology address list");
                return;
            }
            let node = Node::new(
                node, 
                LABEL_MAILBOX, 
                format!("{}:{}", host, port).as_str()
            );
            topo.add_node(group, node.unwrap());
        });
        let network = Network::restore(topo);
        Ok(Self {
            conf: Arc::new(TopoConf::default()),
            network: Arc::new(network),
        })
    }
}

impl Topo {

    #[inline]
    pub fn network(&self) -> &Arc<Network<GroupID, NodeID>> {
        &self.network
    }

    pub fn from_group(group: &GroupProto, blacklist: HashSet<NodeID>) -> Self {
        let GroupProto { id, endpoints, .. } = group;
        let cluster = Network::new();
        for endpoint in endpoints {
            let endpoint = endpoint.try_into();
            if let Err(_) = endpoint {
                continue;
            }
            let node: Node<NodeID> = endpoint.unwrap();
            if blacklist.contains(&node.id) {
                continue;
            }
            cluster.get_topo().add_node(*id, node);
        }
        Self {
            conf: Arc::new(TopoConf::default()),
            network: Arc::new(cluster),
        }
    }
}

/// Parse the group address that in format
/// "group1-(id1-host1:port1, id2-host2:port2); group2-\[id1-host1:port1, id2-host2:port2\];"
pub fn parse_group_address<C>(group_list: String, mut collector: C)
where
    C: FnMut(GroupID, NodeID, Host, Port),
{
    let group_list = group_list.trim();
    if group_list.is_empty() {
        return;
    }

    let groups = group_list.split(';');
    for group in groups {
        let group = group.trim();
        if group.is_empty() {
            continue;
        }
        let split_token = group.find('-');
        if split_token.is_none() {
            warn!(
                "incorrect group: {:?}, format of group should like: groupid-[address1,address2], 
                there must be `-` between groupid and address list",
                group
            );
            continue;
        }
        let split_token = split_token.unwrap();
        let next = split_token + 1;
        if group.len() == next {
            warn!(
                "incorrect group: {:?}, please give address list of this group",
                group
            );
            continue;
        }
        let groupid = &group[0..split_token];
        let groupid = groupid.parse::<GroupID>();
        if let Err(_err) = groupid {
            warn!("invalid groupid: {:?}, required type-u32.", &group);
            continue;
        }
        let groupid = groupid.unwrap();
        let addresses: &str = &group[next..];
        if !valid_str_list(addresses) {
            continue;
        }
        let addresses = addresses.to_owned();
        let end = addresses.len() - 1;
        let addresses = addresses.get(1..end);
        if addresses.is_none() {
            continue;
        }
        let addresses = addresses.unwrap().to_string();
        parse_address(addresses, |node_id, host, port| {
            collector(groupid, node_id, host, port)
        });
    }
}

/// Parse the address that in format "id1-host1:port1, id2-host2:port2"
pub fn parse_address<C>(addr_list: String, mut collector: C)
where
    C: FnMut(NodeID, Host, Port),
{
    let addr_list = addr_list.trim();
    if addr_list.is_empty() {
        return;
    }
    let peers = addr_list.split(',');

    // resolve each peer with id.
    for peer in peers {
        let peer = peer.trim();
        if peer.is_empty() {
            continue;
        }
        // split id-address with token `-`
        let split_token = peer.find('-');
        if split_token.is_none() {
            warn!("incorrect peer: {:?}", peer);
            continue;
        }
        let split_token = split_token.unwrap();
        let next = split_token + 1;
        if peer.len() == next {
            warn!("incorrect peer: {:?}, please give address", peer);
            continue;
        }

        // try resolving peer id.
        let node_id = &peer[0..split_token];
        let node_id = node_id.parse::<NodeID>();
        if let Err(_err) = node_id {
            warn!("invalid initial peer id: {:?}, required type-u64.", &peer);
            continue;
        }
        let node_id = node_id.unwrap();

        // try resolving peer host and port.
        let address = &peer[next..];
        let split_addr: Vec<&str> = address.split(':').collect();
        if split_addr.len() != 2 {
            warn!(
                "incorrect address: {:?}, require format like host:port",
                address
            );
            continue;
        }

        let port = split_addr[1].parse::<u16>();
        if let Err(_e) = port {
            warn!(
                "incorrect port: '{:?}', require for number type",
                split_addr[1]
            );
            continue;
        }
        collector(node_id, split_addr[0].to_string(), port.unwrap());
    }
}
