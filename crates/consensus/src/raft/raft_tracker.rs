pub mod progress;
pub mod inflights;
pub mod progress_state;
pub mod configuration;

use std::{usize};
use progress::Progress;
use crate::quorum::{AckedIndexer, Index, Quorum, VoteResult};
use crate::{DefaultHashBuilder, HashMap, HashSet};
use configuration::Cluster;
use crate::confchange::cluster_changer::ChangeRecord;
use crate::confchange::cluster_changer::ChangeType::*;

pub type ProgressMap = HashMap<u64, Progress>;
impl AckedIndexer for ProgressMap {
    fn acked_index(&self, voter_id: u64) -> Option<crate::quorum::Index> {
        self.get(&voter_id).map(|progress| {
            Index {
                index: progress.match_index,
                group_id: progress.commit_group()
            }
        })
    }
}

#[derive(Clone)]
pub struct ProgressTracker {
    /// progress include both learners and voters
    progress: ProgressMap, 
    /// vote records only tracking the vote result of each voters. 
    vote_records: HashMap<u64, bool>,
    /// the max length of inflights in each Progress
    pub(crate) max_inflight: usize,
    group_commit: bool,
    cluster: Cluster,
}

impl ProgressTracker {

    pub fn empty(max_inflight: usize) -> Self {
        Self::initial_capacity(0, 0, max_inflight)
    }

    pub fn initial_capacity(voters: usize, learners: usize, max_inflight: usize) -> Self {
        ProgressTracker {
            progress: HashMap::with_capacity_and_hasher(voters + learners, DefaultHashBuilder::default()),
            vote_records: HashMap::with_capacity_and_hasher(voters, DefaultHashBuilder::default()),
            max_inflight, group_commit: false,
            cluster: Cluster::with_capacity(voters, learners)
        }
    }

    /// Fetch all voters ids (iterator) from quorum
    /// ## Returns
    /// * iterator: all voter ids' iterator
    /// ## Example
    /// ```
    /// for voter_id in tracker.all_voters() {
    ///      // do something
    /// }
    /// ```
    #[inline] pub fn all_voters(&self) -> impl Iterator<Item = u64> + '_{
        self.quorum.ids()
    }

    #[inline] pub fn all_progress(&self) -> &ProgressMap {
        &self.progress
    }

    #[inline] pub fn enable_group_commit(&mut self, enable: bool) {
        self.group_commit = enable;
    }

    #[inline] pub fn group_commit(&self) -> bool {
        self.group_commit
    }

    /// Assign peers to specific groups in batch
    /// 
    /// ## Params
    /// * assignments: Assignments [(peer_id, assign_group) ...]
    #[inline]
    pub fn assign_commit_groups(&mut self, assignments: &[(u64, u64)]) {
        for (peer_id, assign_group) in assignments {
            if let Some(progress) = self.progress.get_mut(peer_id) {
                progress.assign_commit_group(*assign_group);
            }
        }
    }

    /// Returns an iterator across all the nodes and their progress.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&u64, &Progress)> {
        self.progress.iter()
    }

    /// Returns a mutable iterator across all the nodes and their progress.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn iter_mut(&mut self) -> impl ExactSizeIterator<Item = (&u64, &mut Progress)> {
        self.progress.iter_mut()
    }

    /// Detect if quorum is only 1 peer in joint.
    #[inline]
    pub fn is_standalone_mode(&self) -> bool {
        self.cluster.quorum.is_standalone_mode()
    }

    pub fn clear(&mut self) {
        self.progress.clear();
        self.cluster.clear();
        self.vote_records.clear();
    }
    
    #[inline(always)]
    pub fn cluster_info(&self) -> &Cluster {
        &self.cluster
    }

    pub fn apply_cluster_changes(&mut self, updated_cluster: Cluster, changes: Vec<ChangeRecord>, from_next_idx: u64) {
        self.cluster = updated_cluster;
        for change in changes.iter() {
            let peer_id = change.get_peer();
            match change.get_type() {
                AddPeer => {
                    let mut new_progress = Progress::new(from_next_idx, self.max_inflight);
                    new_progress.recent_active = true;
                    self.progress.insert(peer_id, new_progress);
                },
                RemovePeer => {
                    self.progress.remove(&peer_id);
                }
            }
        }
    }
}

/// Tally vote result of an election (vote_records table)
/// ## Params
/// * "granted" vote number
/// * "reject" vote number
/// * VoteResult after tally vote from vote_records
pub type TallyVoteResult = (usize, usize, VoteResult);

/// The features of the Raft progress manager.
/// This manager will manage all raft nodes in cluster
/// and provide some functions, e.g. determine quorum of 
/// these given raft nodes, record vote of these rafts etc...
pub trait RaftManager {

    /// Grabs a reference to the progress of a node.
    fn get(&self, id: u64) -> Option<&Progress>;

    /// Grabs a mutable reference to the progress of a node.
    fn get_mut(&mut self, id: u64) -> Option<&mut Progress>;

    fn record_vote(&mut self, peer_id: u64, vote: bool);

    /// TallyVotes returns the number of granted and rejected Votes, and whether the
    /// election outcome is known.
    /// ## Returns
    /// * TallyVoteResult (granted, reject, VoteResult)
    fn tally_votes(&self) -> TallyVoteResult;

    /// Clear the records (counting via `record_vote`) from the vote table
    fn reset_votes(&mut self);

    /// Returns the Candidate's eligibility in the current election.
    ///
    /// If it is still eligible, it should continue polling nodes and checking.
    /// Eventually, the election will result in this returning either `Elected`
    /// or `Ineligible`, meaning the election can be concluded.
    fn vote_result(&self, votes: &HashMap<u64, bool>) -> VoteResult;

    /// check all progresses in state machine and detect if 
    /// quorum still active (majority progress's recent_active, include me)
    /// then reset recent_active of these progress (exclude me) to false. 
    fn quorum_recently_active(&mut self, perspective_of: u64) -> bool;

    /// Get the persisted committed index of joint quorum (most of rafts in cluster)
    /// ## Returns
    /// * (committed_index, group_commit_enable)
    fn quorum_committed_index(&self) -> (u64, bool);

    /// Determine if a quorum is formed from the given set of nodes.
    ///
    /// This is the only correct way to verify you have reached a quorum for the whole group.
    /// ## Params
    /// * raft_set: the `potential quorum` formed from set of rafts
    /// ## Returns
    /// * has_reached: given raft set maybe reached the quorum, maybe not
    fn has_reached_quorum(&self, raft_set: &HashSet<u64>) -> bool;
}

impl RaftManager for ProgressTracker {
    
    #[inline]
    fn get(&self, id: u64) -> Option<&Progress> {
        self.progress.get(&id)
    }
    
    #[inline]
    fn get_mut(&mut self, id: u64) -> Option<&mut Progress> {
        self.progress.get_mut(&id)
    }

    fn record_vote(&mut self, peer_id: u64, vote: bool) {
        self.vote_records.entry(peer_id).or_insert(vote);
    }

    fn tally_votes(&self) -> TallyVoteResult {
        let (mut granted, mut reject) = (0, 0);
        for (voter_id, vote) in self.vote_records.iter() {
            if self.quorum.contain_voter(*voter_id) {
                if *vote {
                    granted += 1;
                } else {
                    reject += 1;
                }
            }
        }
        (granted, reject, self.vote_result(&self.vote_records))
    }

    #[inline]
    fn reset_votes(&mut self) {
        self.vote_records.clear();
    }

    fn vote_result(&self, votes: &HashMap<u64, bool>) -> VoteResult {
        self.quorum.vote_result(|voter_id| {
            votes.get(&voter_id).cloned()
        })
    }

    fn quorum_recently_active(&mut self, perspective_of_raft: u64) -> bool {
        let mut active = HashSet::with_capacity_and_hasher(
            self.progress.len(), 
            crate::DefaultHashBuilder::default()
        );

        for (peer_id, progress) in &mut self.progress {
            if *peer_id == perspective_of_raft {
                progress.recent_active = true;
                active.insert(*peer_id);
            } else if progress.recent_active {
                active.insert(*peer_id);
                progress.recent_active = false;
            }
        }
        self.has_reached_quorum(&active)
    }

    fn quorum_committed_index(&self) -> (u64, bool) {
        self.quorum.committed_index(self.group_commit, &self.progress)
    }

    #[inline]
    fn has_reached_quorum(&self, raft_set: &HashSet<u64>) -> bool {
        self.quorum.vote_result(|voter| {
            raft_set.get(&voter).map(|_| {true})
        }) == VoteResult::Won
    }
}