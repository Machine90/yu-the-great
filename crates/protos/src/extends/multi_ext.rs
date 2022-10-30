use std::{collections::{HashMap, BinaryHeap, HashSet}};

use prost::{bytes::BytesMut, Message};
use serde::{Deserialize, ser, de};

use crate::{multi_proto::{BalanceCmd, Assignment, BatchMessages}, raft_group_proto::{EndpointProto, GroupProto}};

impl serde::Serialize for BalanceCmd {
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

impl<'de> Deserialize<'de> for BalanceCmd
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let buf: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let d = BalanceCmd::decode(&buf[..]);
        if let Err(e) = d {
            return Err(de::Error::custom(e));
        }
        Ok(d.unwrap())
    }
}

pub struct Advancer {
    pub assignment: Assignment,
    padded_endpoints: HashMap<u64, EndpointProto>,
    groups: BinaryHeap<GroupProto>,
}

impl Advancer {
    pub fn next_group(&mut self) -> Option<GroupProto> {
        let group = self.groups.pop();
        if group.is_none() {
            if !self.padded_endpoints.is_empty() {
                self.padded_endpoints.clear();
            }
            return None;
        }
        let mut group = group.unwrap();
        if let Some(voters) = group.confstate.as_ref().map(|cs| &cs.voters) {
            for voter in voters {
                if let Some(e) = self.padded_endpoints.get(voter) {
                    group.endpoints.push(e.clone());
                }
            }
        }
        Some(group)
    }
}

pub struct SplitCmd {
    cmd: BalanceCmd,
    endpoints: HashSet<EndpointProto>,
}

impl SplitCmd {
    pub fn left(mut self, mut group: GroupProto) -> Self {
        if !group.endpoints.is_empty() {
            let endpoints = group.take_endpoints();
            self.endpoints.extend(endpoints);
        }
        self.cmd.groups.push(group);
        self
    }

    pub fn to_cmd(self) -> BalanceCmd {
        let Self { cmd, .. } = self;
        cmd
    }
}

impl BalanceCmd {
    pub fn split() -> SplitCmd {
        let split = Self { 
            assignment: Assignment::Split.into(), 
            ..Default::default()
        };
        SplitCmd { cmd: split, endpoints: HashSet::new() }
    }

    pub fn assign_group() -> Self {
        Self { 
            assignment: Assignment::Create.into(), 
            ..Default::default()
        }
    }

    pub fn advance(self) -> Advancer {
        let assignment = self.assignment();
        let BalanceCmd {
            groups, padded_endpoints, ..
        } = self;

        let mut endpoints = HashMap::new();
        for e in padded_endpoints {
            endpoints.insert(e.id, e);
        }
        let mut sorted = BinaryHeap::new();
        for group in groups {
            sorted.push(group);
        }
        Advancer { assignment, padded_endpoints: endpoints, groups: sorted }
    }
}


impl serde::Serialize for BatchMessages {
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

impl<'de> Deserialize<'de> for BatchMessages
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let buf: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let d = BatchMessages::decode(&buf[..]);
        if let Err(e) = d {
            return Err(de::Error::custom(e));
        }
        Ok(d.unwrap())
    }
}

#[cfg(test)] mod tests {
    use std::collections::HashMap;

    #[test] fn test_cmd() {
        let v1 = vec![1,2,3];
        let v2 = vec![1,2,3];
        let mut mp = HashMap::new();
        mp.insert(&v1, 1);
        let eq = mp.contains_key(&v2);
        println!("eq? {eq}");
    }
}