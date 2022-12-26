use std::{
    convert::{TryInto}, 
    collections::HashSet, 
    path::Path, 
    fs::{OpenOptions}, 
    io::{Write, Read, ErrorKind, Error}, 
    str::FromStr, hash::Hash, ops::Range
};

#[allow(unused)]
use prost::{Message, bytes::BytesMut};
use torrent::{topology::node::Node, partitions::key::RefKey};
use vendor::prelude::{DashSet, local_dns_lookup, DashMap};

use crate::{bincode, raft_group_proto::{PortProto, EndpointProto, GroupProto, Estimate}, raft_log_proto::ConfState};
use serde::{Deserialize, ser, de};

pub const RAFT_MAILBOX: &'static str = "raft";

const ID: &str = "id";
const APPLIED: &str = "applied";
const VOTER: &str = "voter";
const LEARNER: &str = "learner";
const ENDPOINTS: &str = "nodes";

impl From<Node<u64>> for EndpointProto {
    fn from(node: Node<u64>) -> Self {
        let mut ports = Vec::with_capacity(node.ports.len());
        for (label, port) in node.ports {
            ports.push(PortProto { label, port: port as u32 });
        }
        let mut labels = Vec::with_capacity(node.labels.len());
        for label in node.labels {
            labels.push(label);
        }
        EndpointProto {
            id: node.id,
            host: node.ori_addr,
            ports, 
            labels,
        }
    }
}

impl From<&Node<u64>> for EndpointProto {
    fn from(node: &Node<u64>) -> Self {
        let mut ports = Vec::new();
        for port in node.ports.iter() {
            let label = port.key().clone();
            let port = *port.value();
            ports.push(PortProto { label, port: port as u32 });
        }
        let mut labels = Vec::new();
        for label in node.labels.iter() {
            labels.push(label.clone());
        }
        EndpointProto {
            id: node.id,
            host: node.ori_addr.clone(),
            ports, labels,
        }
    }
}

/// Fully convert a enpoint to node with u64-type id.
/// this will try looking up address from `EndpointProto`'s 
/// host, so it may cost more time to convert to `Node<u64>`.
impl TryInto<Node<u64>> for EndpointProto {
    type Error = std::io::Error;

    fn try_into(self) -> Result<Node<u64>, Self::Error> {
        let addr = self.host;
        let ports = DashMap::default();
        for port in self.ports {
            let PortProto { label, port } = port;
            ports.insert(label, port as u16);
        }
        let labels = DashSet::default();
        for label in self.labels {
            labels.insert(label);
        }
        let node = Node {
            id: self.id,
            ori_addr: addr.clone(),
            socket_addr: local_dns_lookup(addr.as_str())?,
            ports,
            labels,
        };
        Ok(node)
    }
}

impl TryInto<Node<u64>> for &EndpointProto {
    type Error = std::io::Error;

    fn try_into(self) -> Result<Node<u64>, Self::Error> {
        let addr = self.host.clone();
        let ports = DashMap::default();
        for port in self.ports.iter() {
            let PortProto { label, port } = port;
            ports.insert(label.clone(), *port as u16);
        }
        let labels = DashSet::default();
        for label in self.labels.iter() {
            labels.insert(label.clone());
        }
        let socket_addr = local_dns_lookup(addr.as_str())?;
        let node = Node {
            id: self.id,
            ori_addr: addr,
            socket_addr: socket_addr,
            ports,
            labels,
        };
        Ok(node)
    }
}

#[inline]
pub fn valid_str_list(list: &str) -> bool {
    if list.len() < 2 {
        return false;
    }
    let l = list.chars().next().unwrap(); 
    let r = list.chars().last().unwrap();
    if (l == '[' && r == ']') || (l == '('  && r == ')') || (l == '{'  && r == '}') {
        return true;
    }
    false
}

pub fn to_u64(num: &str) -> std::io::Result<u64> {
    let u64num = num.trim().parse::<u64>();
    if let Err(e) = u64num {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput, 
            format!("parse u64({}) error, see: {:?}", num, e)
        ));
    }
    Ok(u64num.unwrap())
}

pub fn to_group_id(id: &str) -> std::io::Result<u32> {
    let group_id = id.trim().parse::<u32>();
    if let Err(e) = group_id {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput, 
            format!("parse group id: {} error, see: {:?}", id, e)
        ));
    }
    Ok(group_id.unwrap())
}

impl TryInto<String> for EndpointProto {
    type Error = std::io::Error;

    fn try_into(self) -> Result<String, Self::Error> {
        let EndpointProto { id, host, ports, .. } = self;
        let port = ports.iter().find(|&p| {
            p.label == RAFT_MAILBOX
        });
        if port.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound , 
                format!("{} port must be included in node before encode to string", RAFT_MAILBOX)
            ));
        }
        Ok(format!("{}-{}://{}:{}", id, RAFT_MAILBOX, host, port.unwrap().port))
    }
}

impl FromStr for EndpointProto {
    type Err = std::io::Error;

    // 1-rpc://localhost:8080,
    fn from_str(node: &str) -> Result<Self, Self::Err> {
        let node = node.trim();
        if node.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput, 
                format!("node must be not empty, e.g. 1-http://127.0.0.1:80")
            ));
        }
        let split_token = node.find('-');
        if split_token.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput, 
                format!("invalid node format, e.g. 1-http://127.0.0.1:80")
            ));
        }
        let split_token = split_token.unwrap();
        let next = split_token + 1;
        if node.len() == next {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput, 
                format!("node address must be assigned, e.g. 1-http://127.0.0.1:80")
            ));
        }
        let id = &node[0..split_token];
        let id = to_u64(id)?;
        let node: Node<u64> = Node::parse(id, &node[next..])?;
        Ok(node.into())
    }
}

impl Eq for EndpointProto {}

impl Hash for EndpointProto {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Estimate {

    #[inline]
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.used > self.total {
            return Err("group estimate usage has overflowed".into());
        }
        Ok(())
    }

    /// predict how much time(in ms) will take to reached 
    /// the total quota from current used.
    pub fn predict_exceed(&self, percentage: f32) -> Result<u64, Box<dyn std::error::Error>> {
        self.validate()?;
        if percentage > 1. || percentage == 0. {
            return Err("percentage should in range (0.0, 1.0)".into());
        }
        if self.throughput_ms <= 0 {
            return Err("write throughput is negative".into());
        }
        let watermark = (self.total as f64 * percentage as f64).ceil() as u64;
        let speed = self.throughput_ms as u64;
        Ok((watermark - self.used) / speed)
    }
}

impl GroupProto {

    #[inline]
    pub fn update_estimate(&mut self, estimate: Estimate) -> &mut Self {
        self.estimate = Some(estimate);
        self
    }

    pub fn update_applied(&mut self, applied: u64) -> bool {
        let mut updated = false;
        if self.applied < applied {
            self.applied = applied;
            updated = true;
        }
        updated
    }

    #[inline]
    pub fn set_key_range<K: AsRef<[u8]>>(&mut self, range: Range<K>) -> &mut Self {
        let Range { start, end } = range;
        let from = start.as_ref().to_vec();
        let to = end.as_ref().to_vec();
        self.from_key = from;
        self.to_key = to;
        self
    }

    #[inline]
    pub fn set_voters<V: Into<ConfState>>(&mut self, voters: V) -> &mut Self {
        let cs = voters.into();
        self.confstate = Some(cs);
        self
    }

    #[inline]
    pub fn get_voters(&self) -> Option<HashSet<u64>> {
        self.confstate.as_ref().map(|cs| {
            cs.all_voters()
        })
    }

    #[inline]
    pub fn take_endpoints(&mut self) -> Vec<EndpointProto> {
        std::mem::take(&mut self.endpoints)
    }

    /// Complete group with:
    /// * calculate confstate from nodes if absent.
    pub fn complete(mut self) -> Self {
        if self.confstate.is_none() {
            let voters: Vec<u64> = self.endpoints.iter().map(|node| node.id).collect();
            self.confstate = Some(ConfState {
                voters,
                ..Default::default()
            });
        }
        self
    }

    /// Only clone id, from_key, to_key from group.
    #[inline]
    pub fn light_clone(&self) -> Self {
        Self { 
            id: self.id, 
            from_key: self.from_key.clone(), 
            to_key: self.to_key.clone(), 
            ..Default::default()
        }
    }

    /// Check if the key is in this group.
    #[inline]
    pub fn in_range<K: AsRef<[u8]>>(&self, key: K) -> bool {
        let key = key.as_ref();
        let in_bound = key >= self.from_key.as_slice();
        in_bound && if self.to_key.is_empty() {
            true
        } else {
            key < self.from_key.as_slice()
        }
    }

    /// Check if a node is voter of this group.
    pub fn is_voter(&self, expected: u64) -> bool {
        self.confstate.iter().find(|&cs| {
            for voter in &cs.voters {
                if *voter == expected {
                    return true;
                }
            }
            false
        }).is_some()
    }

    #[inline]
    pub fn keys<'a>(&'a self) -> Range<RefKey<'a>> {
        let start = RefKey::left(self.from_key.as_slice());
        let end = RefKey::right(self.to_key.as_slice());
        Range { start, end }
    }

    pub fn add_node(&mut self, node: Node<u64>) {
        let node: EndpointProto = node.into();
        self.endpoints.push(node);
    }

    /// Validate if this group is valid, these
    /// fields will be checked:
    /// * confstate & nodes: check if node ids matched voters union learners.
    /// * \[from_key, to_key): check if they have correct order.
    pub fn validate(&self, is_multi: bool) -> bool {
        // check if multi-group's from_key, to_key has correct order.
        if is_multi && self.from_key >= self.to_key {
            return false;
        }

        // check if voters and learners node exists.
        let cs = &self.confstate;
        if cs.is_none() {
            return false;
        }
        let cs = cs.as_ref().unwrap();
        let mut peers: HashSet<u64> = cs.voters.iter().map(|&v| v).collect();
        for learner in cs.learners.iter() {
            peers.insert(*learner);
        }
        let endpont_ids: HashSet<u64> = self.endpoints.iter().map(|e| e.id).collect();
        if peers != endpont_ids {
            return false;
        }
        true
    }
}

impl GroupProto {

    pub fn from_file<P: AsRef<Path>>(from: P) -> std::io::Result<Self> {
        let mut this = Self::default();
        this.load(from)?;
        Ok(this)
    }

    #[inline]
    pub fn save<P: AsRef<Path>>(&self, to: P) -> std::io::Result<usize> {
        let save_file: &Path = to.as_ref();
        self.save_as(save_file, _guess_file_type(save_file))
    }

    pub fn save_as<P: AsRef<Path>>(&self, to: P, tp: FileType) -> std::io::Result<usize> {
        let save_file: &Path = to.as_ref();
        let to_write = self._encode(tp)?;
        let mut target = OpenOptions::new();
        target.write(true).truncate(true);
        if !save_file.exists() {
            target.create_new(true);
        }
        let mut file = target.open(save_file)?;
        file.write(&to_write[..])
    }

    #[inline]
    pub fn load<P: AsRef<Path>>(&mut self, from: P) -> std::io::Result<()> {
        let load_file: &Path = from.as_ref();
        self.load_as(load_file, _guess_file_type(load_file))
    }

    pub fn load_as<P: AsRef<Path>>(&mut self, from: P, tp: FileType) -> std::io::Result<()> {
        let load_file: &Path = from.as_ref();
        let mut target = OpenOptions::new()
            .read(true)
            .open(load_file)?;

        let mut buf = vec![];
        target.read_to_end(&mut buf)?;
        self._decode(buf, tp)?;
        Ok(())
    }

    #[allow(unused)]
    fn _encode(&self, ft: FileType) -> std::io::Result<Vec<u8>> {
        let GroupProto { 
            id, 
            applied,
            endpoints, 
            confstate, 
            .. } = self;
        match ft {
            FileType::Protobuf => {
                let bin = bincode::serialize(self);
                if let Err(e) = bin {
                    return Err(Error::new(
                        ErrorKind::InvalidInput, 
                        format!("codec error: {:?}", e)
                    ));
                }
                Ok(bin.unwrap())
            },
            FileType::Text => {
                let mut eps = vec![];
                for node in endpoints {
                    let node = node.clone().try_into();
                    if node.is_err() {
                        continue;
                    }
                    let node: String = node.unwrap();
                    eps.push(node);
                }
                let mut buff = String::new();
                buff.push_str(format!("{ID}={}\n", id).as_str());
                buff.push_str(format!("{APPLIED}={}\n", applied).as_str());
                if let Some(cs) = confstate {
                    buff.push_str(format!("{VOTER}={:?}\n", cs.voters).as_str());
                    buff.push_str(format!("{LEARNER}={:?}\n", cs.learners).as_str());
                }
                buff.push_str(format!("{ENDPOINTS}={}", eps.join(",")).as_str());
                Ok(buff.as_bytes().to_vec())
            },
        }
    }

    #[allow(unused)]
    fn _decode(&mut self, content: Vec<u8>, ft: FileType) -> std::io::Result<()>  {
        match ft {
            FileType::Protobuf => {
                let proto = bincode::deserialize::<GroupProto>(&content[..]);
                    if let Err(e) = proto {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput, 
                            format!("codec error: {:?}", e)
                        ));
                    }
                    *self = proto.unwrap();
                    return Ok(())
            },
            FileType::Text => {
                let str_content = String::from_utf8(content);
                if let Err(e) = str_content {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput, 
                        format!("incorrect encoded content when reading group_info in `Text` type, see: {:?}", e)
                    ));
                }
                let str_content = str_content.unwrap();
                for line in str_content.split('\n') {
                    let line = line.trim();
                    let splits: Vec<&str> = line.split("=").collect();
                    if splits.len() != 2 {
                        continue;
                    }
                    let key = splits[0].trim();
                    let value = splits[1].trim();
                    match key.to_lowercase().as_str() {
                        ID => {
                            self.id = to_group_id(value)?;
                        },
                        APPLIED => {
                            self.applied = to_u64(value)?;
                        },
                        VOTER => {
                            if !valid_str_list(value) {}
                            let mut voters = vec![];
                            for v in value[1..value.len() - 1].split(",") {
                                let v = v.trim();
                                if v.is_empty() {
                                    continue;
                                }
                                voters.push(to_u64(v)?);
                            }
                            let mut cs = self.confstate.take().unwrap_or_default();
                            cs.voters = voters;
                            self.confstate = Some(cs);
                        },
                        LEARNER => {
                            if !valid_str_list(value) {}
                            let mut learners = vec![];
                            for l in value[1..value.len() - 1].split(",") {
                                let l = l.trim();
                                if l.is_empty() {
                                    continue;
                                }
                                learners.push(to_u64(l)?);
                            }

                            let mut cs = self.confstate.take().unwrap_or_default();
                            cs.learners = learners;
                            self.confstate = Some(cs);
                        },
                        ENDPOINTS => {
                            let mut endpoints = vec![];
                            for endpoint in value.split(",") {
                                endpoints.push(EndpointProto::from_str(endpoint)?);
                            }
                            self.endpoints = endpoints;
                        }
                        _ => ()
                    }
                }
            },
        }
        Ok(())
    }
}

pub enum FileType {
    /// File with "pb", "protobuf", "ptb" and "bin" also seem as protobuf binary file.
    Protobuf,
    /// Normal text file
    Text,
}

fn _guess_file_type(file: &Path) -> FileType {
    let file_ext = file.extension();
    match file_ext {
        Some(ext) => {
            let ext = ext.to_str();
            if ext.is_none() { return FileType::Text; }
            match ext.unwrap().to_lowercase().as_str() {
                "p" | "pb" | "protobuf" | "ptb" | "bin" => FileType::Protobuf,
                _ => FileType::Text
            }
        },
        None => FileType::Text,
    }
}

impl serde::Serialize for GroupProto {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer
    {
        let mut buf = BytesMut::new();
        let e = self.encode(&mut buf);
        if let Err(e) = e {
            return Err(ser::Error::custom(e));
        }
        let ser = serializer.serialize_bytes(&buf[..])?;
        Ok(ser)
    }
}

impl<'de> Deserialize<'de> for GroupProto
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let buf: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let d = GroupProto::decode(&buf[..]);
        if let Err(e) = d {
            return Err(de::Error::custom(e));
        }
        Ok(d.unwrap())
    }
}

impl Eq for GroupProto {}

impl PartialOrd for GroupProto {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        RefKey::left(&self.from_key)
            .partial_cmp(&RefKey::left(&other.from_key))
            .map(|o| o.reverse())
    }
}

impl Ord for GroupProto {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        RefKey::left(&self.from_key)
            .cmp(&RefKey::left(&other.from_key))
            .reverse()
    }
}

#[cfg(test)] mod tests {
    use crate::{
        raft_group_proto::{
            EndpointProto, PortProto, GroupProto, Estimate
        }, 
        prelude::raft_group_ext::RAFT_MAILBOX, 
        raft_log_proto::ConfState
    };

    #[test] fn test_to_file() {
        let e1 = EndpointProto {
            id: 1,
            host: "localhost".to_owned(),
            ports: vec![ PortProto { label: RAFT_MAILBOX.to_owned(), port: 50071 }, PortProto { label: "http".to_owned(), port: 8080 } ],
            labels: vec![],
        };
        let e2 = EndpointProto {
            id: 2,
            host: "localhost".to_owned(),
            ports: vec![ PortProto { label: RAFT_MAILBOX.to_owned(), port: 50072 }, PortProto { label: "http".to_owned(), port: 8080 } ],
            labels: vec![],
        };
        let e3 = EndpointProto {
            id: 3,
            host: "localhost".to_owned(),
            ports: vec![ PortProto { label: RAFT_MAILBOX.to_owned(), port: 50073 }, PortProto { label: "http".to_owned(), port: 8080 } ],
            labels: vec![],
        };
        let group = GroupProto {
            id: 1,
            applied: 50,
            from_key: b"aaa".to_vec(),
            to_key: b"bbb".to_vec(),
            endpoints: vec![e1,e2,e3],
            confstate: Some(ConfState {
                voters: vec![1,2,3],
                ..Default::default()
            }),
            ..Default::default()
        };
        let r = group.save("../../target/test.pb");
        println!("{:?}", r);
        let r = group.save("../../target/test");
        println!("{:?}", r);

        let mut group2 = GroupProto::default();
        let _r = group2.load("../../target/test");
        println!("{:?}", group2);

        println!("{:?}", (group.is_voter(4), group.is_voter(2)));
    }

    #[test] fn load() {
        let mut group2 = GroupProto::default();
        let _r = group2.load("../../target/test.pb");
        println!("{:?}", group2);
    }

    #[test] fn estimate() {
        let est = Estimate { 
            used: 73728, // used 72KB
            total: 125829120, // total 120MB quota
            throughput_ms: 7168 // increase 7KB per ms
        };
        assert!((7168 * est.predict_exceed(0.8).unwrap() + 73728) < 125829120);
    }
}
