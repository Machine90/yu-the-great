use std::{fmt::Display, collections::HashSet};

use prost::{bytes::BytesMut};
#[allow(unused)] use prost::Message;
use serde::{Deserialize, ser, de};
use torrent::topology::node::Node;
use crate::{raft_payload_proto::{Message as RaftMessage, StatusProto}};

impl Display for RaftMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut entries_log = String::new();
        let entries_num = self.entries.len();
        if entries_num > 0 && entries_num <= 5 {
            entries_log.push_str(" with entries: ");
            for e in self.entries.iter() {
                let e_index = e.index;
                let e_term = e.term;
                let e_type = e.entry_type();
                let line = format!(
                    "Entry [index: {:?}, term: {:?}, type: {:?}], ",
                    e_index, e_term, e_type
                );
                entries_log.push_str(line.as_str());
            }
        } else if entries_num > 0 {
            let s = self.entries.first().map(|e| (e.term, e.index)).unwrap();
            let e = self.entries.last().map(|e| (e.term, e.index)).unwrap();
            entries_log = format!(" with {} entries from {:?} to {:?}", entries_num, s, e);
        }
        let rej = self.reject;
        let rej_hint = self.reject_hint;
        write!(f, 
            "{:?} from ({:?}) to ({:?}) [index: {:?}, term: {:?}, log_term: {:?}, commit: {:?}, commit_term: {:?}, request_snap: {:?}, reject: {:?} reject_hint: {:?}]{}", 
            self.msg_type(), self.from, self.to, self.index, self.term, self.log_term, self.commit, 
            self.commit_term, self.request_snapshot, rej, rej_hint, entries_log
        )
    }
}

impl serde::Serialize for RaftMessage {
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

impl<'de> Deserialize<'de> for RaftMessage
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let buf: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let d = RaftMessage::decode(&buf[..]);
        if let Err(e) = d {
            return Err(de::Error::custom(e));
        }
        Ok(d.unwrap())
    }
}

impl StatusProto {

    #[inline]
    pub fn with_group_id(mut self, group_id: u32) -> Self {
        self.set_group_id(group_id);
        self
    }
    
    #[inline]
    pub fn set_group_id(&mut self, group_id: u32) {
        self.group_id = group_id;
    }

    pub fn add_endpoints(&mut self, endpoints: &Vec<Node<u64>>) {
        let mut ids: HashSet<_> = self.endpoints.iter().map(|e| e.id).collect();
        for endpoint in endpoints {
            if ids.contains(&endpoint.id) { continue; }
            ids.insert(endpoint.id);
            self.endpoints.push(endpoint.into());
        }
    }

    pub fn append_endpoints(&mut self, endpoints: Vec<Node<u64>>) {
        let mut ids: HashSet<_> = self.endpoints.iter().map(|e| e.id).collect();
        for endpoint in endpoints {
            if ids.contains(&endpoint.id) { continue; }
            ids.insert(endpoint.id);
            self.endpoints.push(endpoint.into());
        }
    }

    pub fn add_endpoint(&mut self, endpoint: &Node<u64>) {
        for e in self.endpoints.iter() {
            if e.id == endpoint.id {
                // existed
                return;
            }
        }
        self.endpoints.push(endpoint.into());
    }
}

impl<'de> Deserialize<'de> for StatusProto
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let buf: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let d = StatusProto::decode(&buf[..]);
        if let Err(e) = d {
            return Err(de::Error::custom(e));
        }
        Ok(d.unwrap())
    }
}

impl serde::Serialize for StatusProto {
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

