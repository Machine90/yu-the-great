use std::cmp::{max, min};

use crate::raft::{DUMMY_ID, DUMMY_INDEX};
use crate::{debug};

use super::{
    inflights::{Inflights, RingBuffer},
    progress_state::ProgressState,
};

/// Progress represents a follower’s progress in the view of the leader. Leader
/// maintains progresses of all followers, and sends entries to the follower
/// based on its progress.
///
/// NB(tbg): Progress is basically a ***State machine*** whose transitions are mostly
/// strewn around `*raft.raft`. Additionally, some fields are only used when in a
/// certain State. All of this isn't ideal.
#[derive(Debug, Clone, PartialEq)]
pub struct Progress {
    /// The next index and mach index in specific follower raft
    /// match_index means the index that follower has been sync with leader
    pub match_index: u64,

    /// and the next_index means the index that leader raft will try to sync
    /// with follower raft at next time
    pub next_index: u64,

    pub state: ProgressState,

    /// ProbeSent is used while this follower is in StateProbe. When ProbeSent is
    /// true, raft should pause sending replication message to this peer until
    /// ProbeSent is reset. See ProbeAcked() and IsPaused()
    pub probe_sent: bool,

    /// This is true if the progress is recently active. Receiving any messages
    /// from the corresponding follower indicates the progress is active.
    /// RecentActive can be reset to false after an election timeout.
    pub recent_active: bool,
    pub pending_snapshot: u64,
    pending_request_snapshot: u64,
    pub inflights: RingBuffer,

    pub committed_index: u64,

    /// Only logs replicated to different group will be committed if any group is configured.
    commit_group_id: u64
}

impl Progress {
    pub fn new(next_index: u64, max_inflights: usize) -> Self {
        Progress {
            match_index: 0,
            next_index,
            state: Default::default(),
            probe_sent: false,
            recent_active: false,
            pending_snapshot: 0,
            pending_request_snapshot: 0,
            inflights: RingBuffer::new(max_inflights),
            committed_index: 0,
            commit_group_id: DUMMY_ID
        }
    }

    #[inline]
    pub fn pending_request_snapshot(&self) -> u64 {
        self.pending_request_snapshot
    }
    
    /// Update this progress's `pending_request_snapshot` value.
    #[inline]
    pub fn request_snapshot(&mut self, request_snap_index: u64) {
        self.pending_request_snapshot = request_snap_index;
    }

    /// Determine if still pending request snapshot in this progress.
    #[inline]
    pub fn still_pending_snapshot_request(&self) -> bool {
        self.pending_request_snapshot != DUMMY_INDEX
    }

    /// Reset `pending_request_snapshot` to 0, means 
    /// there has not any pending snapshot request.
    #[inline]
    pub fn clear_pending_snapshot_request(&mut self) {
        self.pending_request_snapshot = DUMMY_INDEX;
    }

    /// Detect if pause in each state of voters progress. 
    /// 
    /// If current voter's progress:
    /// * When in `Snapshot`: always paused
    /// * When in `Probe`: paused if `probe_sent` is true.
    /// * When in Replicate: paused if this replicated voter's 
    /// messagebox (inflight message) is full.
    #[inline]
    pub fn is_paused(&self) -> bool {
        match self.state {
            ProgressState::Snapshot => true,
            ProgressState::Probe => self.probe_sent,
            ProgressState::Replicate => self.inflights.is_full(),
        }
    }

    /// Pause the `Probe` voter's progress, then Leader would 
    /// not appending to this `Probe` follower.
    #[inline]
    pub fn pause(&mut self) {
        self.probe_sent = true;
    }

    // ProbeAcked is called when this peer has accepted an append. It resets
    // ProbeSent to signal that additional append messages should be sent without
    // further delay.
    #[inline]
    pub fn resume(&mut self) {
        self.probe_sent = false;
    }

    /// (maybe)Update committed index and return the updated value
    pub fn update_committed(&mut self, update_committed: u64) -> u64 {
        if update_committed > self.committed_index {
            self.committed_index = update_committed;
            update_committed
        } else {
            self.committed_index
        }
    }

    /// try_update is called when an ***MsgAppendResponse*** arrives from the follower, with the
    /// index acked by it. The method returns false if the given n index comes from
    /// an outdated message. Otherwise it updates the progress and returns true.
    /// ## Params
    /// * n_index: the index come from follower in the quorum (cluster)
    /// ## Returns
    /// * true if update succeed
    pub fn try_update(&mut self, n_index: u64) -> bool {
        let need_update: bool = self.match_index < n_index;
        if need_update {
            self.match_index = n_index;
            self.resume();
        }
        self.next_index = max(self.next_index, n_index + 1);
        need_update
    }

    // try_decr_to adjusts the Progress to the receipt of a MsgAppend rejection. The
    // arguments are the index of the append message rejected by the follower, and
    // the hint that we want to decrease to.
    //
    // Rejections can happen spuriously as messages are sent out of order or
    // duplicated. In such cases, the rejection pertains to an index that the
    // Progress already knows were previously acknowledged, and false is returned
    // without changing the Progress.
    //
    // If the rejection is genuine, Next is lowered sensibly, and the Progress is
    // cleared for sending log entries.
    pub fn try_decr_to(&mut self, rejected: u64, matched_hint: u64, request_snapshot_idx: u64) -> bool {
        let is_not_snapshot = request_snapshot_idx == DUMMY_INDEX;

        if self.state == ProgressState::Replicate {
            let matched = self.match_index;
            // The rejection must be stale if the progress has matched and "rejected"
		    // is smaller than "match".
            let rejection_stale = rejected < matched;
            
            if rejection_stale || (rejected == matched && is_not_snapshot) {
                return false;
            }

            if is_not_snapshot {
                self.next_index = matched + 1;
            } else {
                // follower request snapshot in MsgAppendResponse
                self.request_snapshot(request_snapshot_idx);
            }
            return true;
        }

        if (self.next_index == 0 || self.next_index - 1 != rejected) && is_not_snapshot {
            return false;
        }

        if is_not_snapshot {
            self.next_index = max(min(rejected, matched_hint + 1), 1);
        } else if self.still_pending_snapshot_request() {
            self.request_snapshot(request_snapshot_idx);
        }
        self.resume();
        true
    }

    /// Push inflight messages to `Progress` and update `next_index` and return
    /// current state of the `State machine`
    #[inline]
    pub fn push_inflight(&mut self, last_index: u64) -> &ProgressState {
        match self.state {
            ProgressState::Snapshot => {
                panic!(
                    "Unexcepted state: {:?} when pushing inflght message index",
                    self.state
                )
            }
            ProgressState::Probe => self.pause(),
            ProgressState::Replicate => {
                self.next_index = last_index + 1;
                self.inflights.push_back(last_index);
            },
        }
        &self.state
    }

    /// Transfer state of current progress.
    #[inline]
    pub fn next_state(&mut self, to_index: u64, of: u64) -> &ProgressState {
        match self.state {
            ProgressState::Snapshot => {
                if self.is_snapshot_abort() {
                    debug!("snapshot aborted, resumed sending replication messages to {from}", from = of);
                    self.enter_probe();
                }
            }
            ProgressState::Probe => self.enter_replicate(),
            ProgressState::Replicate => self.inflights.release_to(to_index),
        };
        &self.state
    }

    pub fn enter_snapshot(&mut self, snapshot_index: u64) {
        self.reset_state(ProgressState::Snapshot);
        self.pending_snapshot = snapshot_index;
    }

    pub fn enter_probe(&mut self) {
        match self.state {
            ProgressState::Snapshot => {
                // If the original state is ProgressStateSnapshot, progress knows that
                // the pending snapshot has been sent to this peer successfully, then
                // probes from pendingSnapshot + 1.
                let stashed_pending_snapshot = self.pending_snapshot;
                self.reset_state(ProgressState::Probe);
                self.next_index = max(stashed_pending_snapshot + 1, self.match_index + 1);
            }
            _ => {
                self.reset_state(ProgressState::Probe);
                self.next_index = self.match_index + 1;
            }
        }
    }

    pub fn enter_replicate(&mut self) {
        self.reset_state(ProgressState::Replicate);
        self.next_index = self.match_index + 1;
    }

    fn reset_state(&mut self, new_state: ProgressState) {
        self.resume();
        self.state = new_state;
        self.pending_snapshot = 0;
        self.inflights.clear();
    }

    pub(crate) fn reset(&mut self, next_index: u64) {
        self.resume();
        self.next_index = next_index;
        self.clear_pending_snapshot_request();
        self.pending_snapshot = 0;
        self.state = ProgressState::default();
        self.match_index = 0;
        self.recent_active = false;
        self.inflights.clear();
    }

    /// If the progress in snapshot state but it's pending snapshot index
    /// is stale, it means that snapshot is abort
    fn is_snapshot_abort(&self) -> bool {
        self.state == ProgressState::Snapshot && self.match_index >= self.pending_snapshot
    }

    pub fn snapshot_failure(&mut self) {
        self.pending_snapshot = DUMMY_INDEX;
    }

    #[inline]
    pub fn assign_commit_group(&mut self, group_id: u64) {
        self.commit_group_id = group_id;
    }

    #[inline]
    pub fn commit_group(&self) -> u64 {
        self.commit_group_id
    }
}
