use std::{convert::TryFrom, ops::{Deref, DerefMut}, hash::Hash, collections::HashSet};

use crate::{protos::prelude::{ConfChangeType, ConfChange}};
use common::{protos::{prelude::BatchConfChange}, protocol::{NodeID}};
use torrent::bincode;

use crate::mailbox::RaftEndpoint;

#[derive(Debug, Clone, Copy)]
pub enum RefChanged<'a> {
    AddNode(&'a RaftEndpoint),
    RemoveNode(NodeID),
}

#[derive(Debug, Clone)]
pub enum Changed {
    AddNode(RaftEndpoint),
    RemoveNode(NodeID),
}

#[derive(Debug)]
pub struct EndpointChange {
    node_id: u64,
    change_type: ConfChangeType,
    endpoint: Option<RaftEndpoint>
}

impl Eq for EndpointChange {}

impl EndpointChange {

    /// A voter has all privillages. it can take part in
    /// an election, append and read_index, leader will try 
    /// to keep consistence with a voter
    pub fn add_as_voter(new: &RaftEndpoint) -> Self {
        Self {
            node_id: new.id,
            change_type: ConfChangeType::AddNode,
            endpoint: Some(new.clone())
        }
    }

    /// Learner can recv Append, Snapshot and Heartbeat, but can't 
    /// join the election, but can't join the commit calculation.
    pub fn add_as_learner(learner: &RaftEndpoint) -> Self {
        Self {
            node_id: learner.id,
            change_type: ConfChangeType::AddLearnerNode,
            endpoint: Some(learner.clone())
        }
    }

    pub fn remove(id: u64) -> Self {
        Self {
            node_id: id,
            change_type: ConfChangeType::RemoveNode,
            endpoint: None
        }
    }

    #[inline]
    pub fn is_add(&self) -> bool {
        self.change_type == ConfChangeType::AddNode || self.change_type == ConfChangeType::AddLearnerNode
    }

    #[inline] 
    pub fn is_remove(&self) -> bool {
        self.change_type == ConfChangeType::RemoveNode
    }

    pub fn get_changed(&self) -> RefChanged {
        match self.change_type {
            ConfChangeType::AddNode | ConfChangeType::AddLearnerNode => {
                RefChanged::AddNode(self.endpoint.as_ref().unwrap())
            },
            ConfChangeType::RemoveNode => {
                RefChanged::RemoveNode(self.node_id)
            },
        }
    }

    pub fn take_changed(self) -> Changed {
        match self.change_type {
            ConfChangeType::AddNode | ConfChangeType::AddLearnerNode => {
                Changed::AddNode(self.endpoint.unwrap())
            },
            ConfChangeType::RemoveNode => {
                Changed::RemoveNode(self.node_id)
            },
        }
    }

    #[inline]
    pub fn opposite(&self) -> ConfChangeType {
        if self.is_add() {
            ConfChangeType::RemoveNode
        } else {
            ConfChangeType::AddNode
        }
    }
}

impl TryFrom<ConfChange> for EndpointChange {
    type Error = std::io::Error;

    fn try_from(change: ConfChange) -> Result<Self, Self::Error> {
        let endpoint = if !change.context.is_empty() {
            let endpoint = bincode::deserialize::<RaftEndpoint>(&change.context);
            if let Err(_) = endpoint {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput, 
                    "invalid RaftEndpoint received from conf_change")
                );
            }
            Some(endpoint.unwrap())
        } else {
            None
        };

        Ok(Self {
            node_id: change.node_id,
            change_type: change.change_type(),
            endpoint
        })
    }
}

impl Into<ConfChange> for EndpointChange {
    fn into(self) -> ConfChange {
        let mut conf_change = ConfChange::default();
        conf_change.set_change_type(self.change_type);
        if let Some(endpoint) = self.endpoint.as_ref() {
            conf_change.context = bincode::serialize(endpoint).unwrap_or(Vec::new());
        }
        conf_change.node_id = self.node_id;
        conf_change
    }
}

impl std::ops::Not for EndpointChange {
    type Output = Self;

    fn not(self) -> Self::Output {
        Self {
            node_id: self.node_id,
            change_type: self.opposite(),
            endpoint: self.endpoint
        }
    }
}

impl PartialEq for EndpointChange {
    fn eq(&self, other: &Self) -> bool {
        let same_op = (self.is_add() && other.is_add()) || (self.is_remove() && other.is_remove());
        self.node_id == other.node_id && same_op
    }
}

impl Hash for EndpointChange {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.node_id.hash(state);
        if self.is_add() {
            let change = ConfChangeType::AddNode;
            change.hash(state);
        } else {
            let change = ConfChangeType::RemoveNode;
            change.hash(state);
        }
    }
}

/// Distinct changes set, repeated Add and Remove will be delete from set.
#[derive(Debug)]
pub struct ChangeSet {
    inner: HashSet<EndpointChange>,
}

impl ChangeSet {
    pub fn new() -> Self {
        Self {
            inner: HashSet::new()
        }
    }

    #[inline]
    pub fn take_list(self) -> HashSet<EndpointChange> {
        self.inner
    }

    pub fn add(mut self, change: EndpointChange) -> Self {
        self.insert(change);
        self
    }

    /// This will request to clear voters from outgoing.
    pub fn leave_joint() -> Self {
        Self::new()
    }

    #[inline]
    pub fn insert(&mut self, change: EndpointChange) -> bool {
        self.push(change, true)
    }

    pub fn push(&mut self, change: EndpointChange, force: bool) -> bool {
        if self.inner.contains(&change) {
            if force {
                self.inner.remove(&change);
            }
            return self.inner.insert(change);
        }
        let ori_type = change.change_type;
        let opposite = !change;
        if self.inner.contains(&opposite) {
            self.inner.remove(&opposite)
        } else {
            let mut ori = !opposite;
            ori.change_type = ori_type;
            self.inner.insert(ori)
        }
    }
}

impl Deref for ChangeSet {
    type Target = HashSet<EndpointChange>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for ChangeSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Into<BatchConfChange> for ChangeSet {
    fn into(self) -> BatchConfChange {
        let mut changes = vec![];
        for change in self.inner {
            changes.push(change.into());
        }
        BatchConfChange {  
            changes, 
            ..Default::default() 
        }
    }
}

#[cfg(test)] mod tests {

    use torrent::topology::node::Node;
    use crate::mailbox::RaftEndpoint;

    use super::{EndpointChange, ChangeSet};


    #[test] fn test_changes() {
        let eps: Vec<RaftEndpoint> = vec![
            Node::parse(1, "rpc://127.0.0.1:50010").unwrap(),
            Node::parse(2, "rpc://127.0.0.1:50011").unwrap(),
            Node::parse(3, "rpc://127.0.0.1:50012").unwrap(),
            Node::parse(4, "rpc://127.0.0.1:50013").unwrap(),
        ];

        let mut ops = ChangeSet::new();
        ops.insert(EndpointChange::add_as_voter(&eps[0]));
        ops.insert(EndpointChange::add_as_voter(&eps[1]));
        ops.insert(EndpointChange::add_as_voter(&eps[2]));
        ops.insert(EndpointChange::add_as_voter(&eps[3]));

        ops.insert(EndpointChange::add_as_learner(&eps[1]));
        ops.insert(EndpointChange::add_as_learner(&eps[2]));

        println!("{:?}", ops);
        println!("-----------------");

        ops.insert(EndpointChange::remove(1));
        ops.insert(EndpointChange::remove(2));
        println!("{:?}", ops);
    }
}