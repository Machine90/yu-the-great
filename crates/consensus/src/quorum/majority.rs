// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// use super::{AckedIndexer, Index, VoteResult};
use crate::{DefaultHashBuilder, HashSet, config::raft_config_vals};

use core::slice;
use std::collections::hash_set::Iter;
use std::fmt::Formatter;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::{cmp, u64};

use super::{AckedIndexer, Index, Quorum, VoteResult};

/// A set of IDs that uses majority quorums to make decisions. impl in Hashset
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Majority {
    voters: HashSet<u64>,
}

impl Quorum for Majority {

    /// Computes the committed index from those supplied via the
    /// provided AckedIndexer (for the active config).
    ///
    /// ## Params
    /// * enable_group_commit: bool 
    /// * acked_voters: &impl AckedIndexer (indexer of voters those who ack the vote)
    /// ## Returns
    /// * u64: quorum index. <br/>
    /// Eg. Disable group commit: If the matched indexes are `[2,2,2,4,5]`, it will return 2.<br/>
    ///     Enable group commit: `[(1, 1), (2, 2), (3, 2)]` return 1. (index, group) <br/>
    /// * bool: final result of `enable_group_commit` it's a calculated value
    fn committed_index(&self, enable_group_commit: bool, acked_voters: &impl AckedIndexer) -> (u64, bool) {
        if self.voters.is_empty() {
            return (u64::MAX, true);
        }
        let mut stk_arr: [MaybeUninit<Index>; raft_config_vals::LIMITED_ALLOC_STK_ARR_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
        let mut heap_arr;
        // record all voter's matched idx value to a sorted list
        let matched = if self.voters.len() <= raft_config_vals::LIMITED_ALLOC_STK_ARR_SIZE {
            // allocate the stack memo to optimize the memory used
            for (i, v) in self.voters.iter().enumerate() {
                stk_arr[i] = MaybeUninit::new(acked_voters.acked_index(*v).unwrap_or_default());
            }
            unsafe {
                slice::from_raw_parts_mut(stk_arr.as_mut_ptr() as *mut _, self.voters.len())
            }
        } else {
            let mut buf = Vec::with_capacity(self.voters.len());
            for v in &self.voters {
                buf.push(acked_voters.acked_index(*v).unwrap_or_default());
            }
            heap_arr = Some(buf);
            heap_arr.as_mut().unwrap().as_mut_slice()
        };

        matched.sort_by(|a, b| b.index.cmp(&a.index));
        // find majority grant index as quorum's commit, e.g. [2,2,2,3,4] then 2 is quorum's commit.
        let quorum = majority(matched.len());
        let quorum_index = matched[quorum - 1];

        // return quorum index imediately if we disable goup commit, just return the quorum index
        if !enable_group_commit { return (quorum_index.index, false); }
        let (quorum_commit_index, mut last_group_id) = (quorum_index.index, quorum_index.group_id);
        let mut is_single_group = true;

        for voter in matched.iter() {
            if voter.group_id == 0 {
                is_single_group = false;
                continue;
            }
            if last_group_id == 0 {
                last_group_id = voter.group_id;
            } else if last_group_id != voter.group_id {
                return (cmp::min(voter.index, quorum_commit_index), true)
            }
        }
        if is_single_group {
            (quorum_commit_index, false)
        } else {
            (matched.last().unwrap().index, false)
        }
    }

    fn vote_result(&self, check: impl Fn(u64) -> Option<bool>) -> VoteResult {
        if self.voters.is_empty() {
            // By convention, the elections on an empty config win. This comes in
            // handy with joint quorums because it'll make a half-populated joint
            // quorum behave like a majority quorum.
            return VoteResult::Won;
        }

        let (mut yes, mut no) = (0, 0);
        for voter in self.voters.iter() {
            match check(*voter) {
                // got ensured vote result from supporter
                Some(true) => yes += 1,
                // the vote maybe not arrived, not meaning reject
                Some(false) => no += 1,
                _ => () // reject
            }
        }
        let quorum_requirement = majority(self.voters.len());
        if yes >= quorum_requirement {
            VoteResult::Won
        } else if no > quorum_requirement {
            VoteResult::Lost
        } else {
            VoteResult::Pending
        }
    }

    #[inline] fn contain_voter(&self, to_peer_id: u64) -> bool {
        self.voters.contains(&to_peer_id)
    }

    #[inline] fn clear(&mut self) {
        self.voters.clear()
    }
}

impl Majority {
    /// Creates a new Majority using the given IDs.
    pub fn new(voters: HashSet<u64>) -> Majority {
        Majority { voters }
    }

    /// Creates an empty Majority with given capacity.
    pub fn with_capacity(cap: usize) -> Majority {
        Majority {
            voters: HashSet::with_capacity_and_hasher(cap, DefaultHashBuilder::default()),
        }
    }

    /// Returns an iterator over voters.
    pub fn ids(&self) -> Iter<'_, u64> {
        self.voters.iter()
    }

    /// Returns the MajorityConfig as a sorted slice.
    pub fn slice(&self) -> Vec<u64> {
        let mut voters = self.raw_slice();
        voters.sort_unstable();
        voters
    }

    /// Returns the MajorityConfig as a slice.
    pub fn raw_slice(&self) -> Vec<u64> {
        self.voters.iter().cloned().collect()
    }
}

/// Calculate quorm of given total.
#[inline] pub fn majority (total: usize) -> usize {
    total / 2 + 1
}

impl Deref for Majority {
    type Target = HashSet<u64>;

    #[inline]
    fn deref(&self) -> &HashSet<u64> {
        &self.voters
    }
}

impl DerefMut for Majority {
    #[inline]
    fn deref_mut(&mut self) -> &mut HashSet<u64> {
        &mut self.voters
    }
}

impl std::fmt::Display for Majority {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "({})",
            self.voters
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<String>>()
                .join(" ")
        )
    }
}