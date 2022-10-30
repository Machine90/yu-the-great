use self::{raft_candidate::CandidateRaft, raft_follower::FollowerRaft, raft_leader::LeaderRaft};
use crate::protos::{raft_payload_proto as payload};
use crate::errors::*;

pub mod raft_candidate;
pub mod raft_follower;
pub mod raft_leader;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum RaftRole {
    /// The node is a leader.
    Leader,
    /// The node could become a leader.
    Candidate,
    /// The node could become a candidate, if enable_pre_vote.
    PreCandidate,
    /// The node is a follower of the leader.
    Follower,
}

impl RaftRole {

    #[inline]
    pub fn is_leader(&self) -> bool {
        match self {
            RaftRole::Leader => true,
            _ => false
        }
    }
}

impl ToString for RaftRole {
    fn to_string(&self) -> String {
        match &self {
            Self::Leader => "Leader".to_owned(),
            Self::Follower => "Follower".to_owned(),
            Self::Candidate => "Candidate".to_owned(),
            Self::PreCandidate => "PreCandidate".to_owned(),
        }
    }
}

impl From<&str> for RaftRole {
    fn from(case: &str) -> Self {
        let lowcase = case.to_lowercase();
        let case = lowcase.as_str();
        match case {
            "leader" => Self::Leader,
            "follower" => Self::Follower,
            "candidate" => Self::Candidate,
            "precandidate" => Self::PreCandidate,
            _ => Self::Follower
        }
    }
}

impl Default for RaftRole {
    fn default() -> Self {
        RaftRole::Follower
    }
}

pub fn try_commit_and_broadcast(leader: &mut dyn LeaderRaft) {
    leader.try_commit_and_broadcast();
}

#[inline]
pub fn broadcast_cluster_conf_change(leader: &mut dyn LeaderRaft) {
    leader.broadcast_cluster_conf_change();
}

pub fn step_leader(leader: &mut dyn LeaderRaft, message: payload::Message) -> Result<()> {
    leader.process(message)
}

pub fn step_follower(follower: &mut dyn FollowerRaft, message: payload::Message) -> Result<()> {
    follower.process(message)
}

pub fn step_candidate(candidate: &mut dyn CandidateRaft, message: payload::Message) -> Result<()> {
    candidate.process(message)
}
