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