pub mod joint;
pub mod majority;

use std::fmt::{self, Debug, Display, Formatter};

use crate::HashMap;

/// Abstraction of quorum, a quorum can be a `joint` (combination of two majority) 
/// or a `majority`, it's always act as a group of raft and provide some functions like
/// determine vote result of this raft group, calculate the committed index of raft group etc...
pub trait Quorum {

    /// Determine if there exists specific voter in joint then true
    fn contain_voter(&self, peer_id: u64) -> bool;

    /// Computes the committed index from those supplied via the
    /// provided AckedIndexer (for the active config).
    ///
    /// ## Params
    /// * enable_group_commit: bool 
    /// * acked_voters: &impl AckedIndexer (indexer of voters those who ack the vote)
    /// ## Returns
    /// * u64: quorum index, the committed index that majority rafts has persisted it. <br/>
    /// Eg. Disable group commit: If the matched indexes are `[2,2,2,4,5]`, it will return 2.<br/>
    ///     Enable group commit: `[(1, 1), (2, 2), (3, 2)]` return 1. (index, group) <br/>
    /// * bool: final result of `enable_group_commit` it's a calculated value
    fn committed_index(&self, enable_group_commit: bool, acked_voters: &impl AckedIndexer) -> (u64, bool);

    fn vote_result(&self, vote_check: impl Fn(u64) -> Option<bool>) -> VoteResult;

    /// Clear the voters in the quorum. E.g. remove them from the vector.
    fn clear(&mut self);
}

/// Enum of vote result, now we provide 3 kinds result type
#[derive(Clone, Copy, PartialEq)]
pub enum VoteResult {
    /// Won indicates that the quorum has voted "yes". "yes" votes more `majority(total: usize)`
    Won,
    /// Pending indicates that the decision of the vote depends on future
    /// votes, i.e. neither "yes" nor "no" has reached quorum yet.
    Pending,
    Lost,
}

#[derive(Default, Clone, Copy)]
pub struct Index {
    pub index: u64,
    pub group_id: u64
}

/// Abstract acked(response for election, for append etc..) raft indexer
pub trait AckedIndexer {
    /// Find acked raft via it's id 
    fn acked_index(&self, voter_id: u64) -> Option<Index>;
}

pub type AckIndexer = HashMap<u64, Index>;

impl AckedIndexer for AckIndexer {

    #[inline] fn acked_index(&self, voter_id: u64) -> Option<Index> {
        self.get(&voter_id).cloned()
    }
}

impl Display for Index {

    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.group_id {
            0 => match self.index {
                u64::MAX => write!(f, "∞"),
                index => write!(f, "{}", index),
            },
            group_id => match self.index {
                u64::MAX => write!(f, "[{}]∞", group_id),
                index => write!(f, "[{}]{}", group_id, index),
            },
        }
    }
}

impl Debug for Index {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for VoteResult {

    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VoteResult::Won => write!(f, "VoteWon"),
            VoteResult::Lost => write!(f, "VoteLost"),
            VoteResult::Pending => write!(f, "VotePending"),
        }
    }
}
