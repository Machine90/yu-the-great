use prost::{bytes::BytesMut, Message};
use serde::{de, ser, Deserialize};
#[allow(unused)]
use torrent::topology::node::Node;

use crate::{
    bincode,
    raft_conf_proto::{BatchConfChange, ConfChange, ConfChangeTransition::*, ConfChangeType},
};

impl ConfChange {
    pub fn new(node_id: u64, change_type: ConfChangeType) -> Self {
        let mut inst = ConfChange::default();
        inst.node_id = node_id;
        inst.set_change_type(change_type);
        inst
    }
}

pub enum ChangeEvent {
    EnterJoint,
    LeaveJoint,
    Simple,
}

impl BatchConfChange {
    /// Determine the change event via the changes
    /// ## Returns
    /// * ChangeEvent
    /// * Option<bool>: auto_leave of the `EnterJoint` event
    /// ## ChangeEvent

    /// * ***EnterJoint:***
    /// It will return Some if and only if this config change will use Joint Consensus,
    /// which is the case if it contains more than one change or if the use of Joint
    /// Consensus was requested explicitly. The bool indicates whether the Joint State
    /// will be left automatically.

    /// * ***LeaveJoint:***
    /// This is the case if the changes of BatchConfChange is zero, with the possible exception of
    /// the Context field.

    /// * ***Simple:***
    /// in theory, more config changes could qualify for the "simple"
    /// protocol but it depends on the config on top of which the changes apply.
    /// For example, adding two learners is not OK if both nodes are part of the
    /// base config (i.e. two voters are turned into learners in the process of
    /// applying the conf change). In practice, these distinctions should not
    /// matter, so we keep it simple and use Joint Consensus liberally.
    pub fn get_change_event(&self) -> (ChangeEvent, Option<bool>) {
        if self.transition() == Auto && self.changes.is_empty() {
            return (ChangeEvent::LeaveJoint, None);
        } else if self.transition() != Auto || self.changes.len() > 1 {
            let enter_joint = match self.transition() {
                Auto | Implicit => (ChangeEvent::EnterJoint, Some(true)),
                Explicit => (ChangeEvent::EnterJoint, Some(false)),
            };
            return enter_joint;
        } else {
            return (ChangeEvent::Simple, None);
        }
    }
}

impl BatchConfChange {
    #[inline]
    pub fn add_voter(&mut self, endpoint: Node<u64>) -> std::io::Result<&mut Self> {
        self._add_change(endpoint, ConfChangeType::AddNode)?;
        Ok(self)
    }

    #[inline]
    pub fn add_learner(&mut self, endpoint: Node<u64>) -> std::io::Result<()> {
        self._add_change(endpoint, ConfChangeType::AddLearnerNode)
    }

    #[inline]
    pub fn remove_node(&mut self, endpoint: Node<u64>) -> std::io::Result<()> {
        self._add_change(endpoint, ConfChangeType::RemoveNode)
    }

    fn _add_change(
        &mut self,
        endpoint: Node<u64>,
        change_tp: ConfChangeType,
    ) -> std::io::Result<()> {
        let node_id = endpoint.id;
        let encoded = bincode::serialize(&endpoint);
        if let Err(e) = encoded {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{:?}", e),
            ));
        }
        let ctx = encoded.unwrap();
        let mut change = ConfChange {
            node_id,
            context: ctx,
            ..Default::default()
        };
        change.set_change_type(change_tp);
        self.changes.push(change);
        Ok(())
    }
}

impl serde::Serialize for BatchConfChange {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
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

impl<'de> Deserialize<'de> for BatchConfChange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let d = BatchConfChange::decode(&buf[..]);
        if let Err(e) = d {
            return Err(de::Error::custom(e));
        }
        Ok(d.unwrap())
    }
}
