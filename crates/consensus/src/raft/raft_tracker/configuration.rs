use std::{ops::Deref, usize};
use crate::protos::raft_log_proto::ConfState;

use crate::{DefaultHashBuilder, HashSet, quorum::{Quorum, majority::Majority}};
use crate::Joint;

use super::ProgressTracker;

#[derive(Clone, Default, PartialEq, Debug)]
pub struct Cluster {
    pub(crate) quorum: Joint,
    pub(crate) learners: HashSet<u64>,
    pub(crate) learners_next: HashSet<u64>,
    pub(crate) auto_leave: bool
}

impl Cluster {

    pub fn new(
        voters: impl IntoIterator<Item = u64>,
        learners: impl IntoIterator<Item = u64>
    ) -> Self {
        Self {
            quorum: Joint::new(voters.into_iter().collect()),
            auto_leave: false,
            learners: learners.into_iter().collect(),
            learners_next: HashSet::default(),
        }
    }

    pub fn with_capacity(voters: usize, learners: usize) -> Self {
        Self {
            quorum: Joint::with_capacity(voters),
            learners: HashSet::with_capacity_and_hasher(learners, DefaultHashBuilder::default()),
            learners_next: HashSet::default(),
            auto_leave: false,
        }
    }
    
    pub fn to_conf_state(&self) -> ConfState {
        let mut conf_state = ConfState::default();
        conf_state.voters = self.quorum.incoming.raw_slice();
        conf_state.voters_outgoing = self.quorum.outgoing.raw_slice();
        conf_state.learners = self.learners.iter().cloned().collect();
        conf_state.learners_next = self.learners_next.iter().cloned().collect();
        conf_state.auto_leave = self.auto_leave;
        conf_state
    }

    pub fn clear(&mut self) {
        self.quorum.clear();
        self.learners.clear();
        self.learners_next.clear();
        self.auto_leave = false;
    }

    #[inline(always)]
    pub fn incoming_mut(&mut self) -> &mut Majority {
        &mut self.quorum.incoming
    }

    #[inline(always)]
    pub fn outgoing_mut(&mut self) -> &mut Majority {
        &mut self.quorum.outgoing
    }

    #[inline(always)]
    pub fn incoming(&self) -> &Majority {
        &self.quorum.incoming
    }

    #[inline(always)]
    pub fn outgoing(&self) -> &Majority {
        &self.quorum.outgoing
    }

    #[inline]
    pub fn make_voter(&mut self, peer_id: u64) {
        self.incoming_mut().insert(peer_id);
        self.learners.remove(&peer_id);
        self.learners_next.remove(&peer_id);
    }

    #[inline]
    pub fn make_learner(&mut self, peer_id: u64) {
        if self.learners.contains(&peer_id) {
            return;
        }
        self.incoming_mut().remove(&peer_id);
        self.learners_next.remove(&peer_id);

        if self.outgoing().contains(&peer_id) {
            self.learners_next.insert(peer_id);
        } else {
            self.learners.insert(peer_id);
        }
    }

    /// Remove node from current quorum, and return 
    /// true if the node is still a voter in outgoing.
    #[inline]
    pub fn remove(&mut self, node_id: u64) -> bool {
        self.incoming_mut().remove(&node_id);
        self.learners.remove(&node_id);
        self.learners_next.remove(&node_id);
        self.outgoing().contains(&node_id)
    }
}

impl Deref for ProgressTracker {

    type Target = Cluster;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}
