
use std::{cmp::min, usize};

use crate::HashSet;
use crate::Majority;

use super::{AckedIndexer, Quorum, VoteResult};
use super::VoteResult::{Won, Lost, Pending};

/// Joint is made up of 2 kinds of marjority, they're incoming and outgoing.
/// Consider in this scenario, a group with peers [1,2,3], we attempt to add 
/// a new peer 4 and remove peer 1, the final group will looks like [2,3,4],
/// now incoming is [2,3,4] and outgoing is [1,2,3], in joint, index and vote 
/// result will be tallied with this two majority.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct Joint {
    /// when a new voter join the quorum, this peer will be added to incoming.
    pub(crate) incoming: Majority,
    /// if a peer attempt to leave quorum, the peer will not be removed directly, 
    /// normaly add to outgoing first.
    pub(crate) outgoing: Majority
}

impl Joint {

    pub fn new(voters: HashSet<u64>) -> Self {
        Joint {
            incoming: Majority::new(voters),
            outgoing: Majority::default()
        }
    }

    /// construct the Joint quorum with given voter numbers
    pub fn with_capacity(voters: usize) -> Self {
        Joint {
            incoming: Majority::with_capacity(voters),
            outgoing: Majority::default()
        }
    }

    pub fn ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.incoming.union(&self.outgoing).cloned()
    }

    /// Detect if quorum is only 1 peer in incoming set.
    #[inline]
    pub fn is_standalone_mode(&self) -> bool {
        self.outgoing.is_empty() && self.incoming.len() == 1
    }

    #[inline]
    pub(crate) fn already_joint(&self) -> bool {
        !self.outgoing.is_empty()
    }
}

impl Quorum for Joint {

    /// Determine if there exists specific voter in joint then true
    fn contain_voter(&self, to_peer_id: u64) -> bool {
        self.incoming.contains(&to_peer_id) || self.outgoing.contains(&to_peer_id)
    }

    /// Find the minimal committed index from quorum1 and quorum2
    fn committed_index(&self, enable_group_commit: bool, acked_voters: &impl AckedIndexer) -> (u64, bool) {
        let (q1_commited_idx, q1_group_commit) = self.incoming.committed_index(enable_group_commit, acked_voters);
        let (q2_commited_idx, q2_group_commit) = self.outgoing.committed_index(enable_group_commit, acked_voters);
        (min(q1_commited_idx, q2_commited_idx), q1_group_commit && q2_group_commit)
    }

    fn vote_result(&self, vote_check: impl Fn(u64) -> Option<bool>) -> VoteResult {
        let vote1 = self.incoming.vote_result(&vote_check);
        let vote2 = self.outgoing.vote_result(vote_check);
        return match (vote1, vote2) {
            (Won, Won) => Won,
            (Lost, _) | (_, Lost) => Lost,
            _ => Pending
        };
    }

    fn clear(&mut self) {
        self.incoming.clear();
        self.outgoing.clear();
    }
}

#[test]
pub fn test() {
}