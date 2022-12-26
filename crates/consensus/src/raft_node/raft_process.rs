use std::mem;

use crate::protos::{raft_payload_proto::{Message}};
use crate::{errors::{Result, Error}, raft::{raft_role::RaftRole, raft_tracker::RaftManager}};

use crate::storage::Storage;

use super::{LightReady, RaftNode, Ready, ReadyRecord};
use crate::{trace};

/// The core processes that raft open to developers
pub trait RaftProcess {

    /// Determine if has some `ready` items should be handled.
    /// * messages: msg to be send (from Leader) or response (from follower)
    /// * soft state: **leader_id** has been changed or role changed
    /// * hard state: commit changed or term changed
    /// * read_states: received `read_index` request and generated some readed result to return
    /// * unstable entries: receive `append` from **leader** then must be persist appended entries
    /// * snapshot: receive a snapshot from **leader**
    /// * commit entries: after update commit, then generate some entries should be apply 
    fn has_ready(&self) -> bool;

    fn step(&mut self, message: Message) -> Result<()>;

    /// Process raft group with given message once and collect ready immediately
    /// ### Suggestions
    /// This method often using to guarantee the `linear consistency` <br/>
    /// If you guys have not demands for consistency, and want high concurrency.
    /// then split this operation to:
    /// 1. `process_message` 
    /// 2. `has_ready`
    /// 3. `get_ready`
    fn step_and_ready(&mut self, message: Message) -> Result<Ready> {
        let processed = self.step(message);
        if let Err(err) = processed {
            return Err(err);
        }
        if !self.has_ready() {
            return Err(Error::Nothing);
        }
        let ready = self.get_ready();
        Ok(ready)
    }

    /// Get ready from raft, if nothing ready, then return error: [Nothing](common::errors::Error::Nothing)
    /// * messages: msg to be send (from Leader) or response (from follower)
    /// * soft state: **leader_id** has been changed or role changed
    /// * hard state: commit changed or term changed
    /// * read_states: received `read_index` request and generated some readed result to return
    /// * unstable entries: receive `append` from **leader** then must be persist appended entries
    /// * snapshot: receive a snapshot from **leader**
    /// * commit entries: after update commit, then generate some entries should be apply 
    fn get_if_ready(&mut self) -> Result<Ready> {
        if !self.has_ready() {
            return Err(Error::Nothing);
        }
        let ready = self.get_ready();
        Ok(ready)
    }

    /// Both get `ready` stuffs (messages, entries) and `light_ready` stuffs
    fn get_ready(&mut self) -> Ready;

    /// This action will call `advance_append` and `advance_apply_to`
    /// ## Noting
    /// All messages should be handle by `Leader` could be get before `advance(ready)`,
    /// other role could get not messages before `advance`.<br/>
    /// all role could get messages after advance ready. <br/>
    /// What kinds of messages should `Leader` handle? e.g. `MsgAppend`, `MsgHeartBeat` etc.. <br/>
    /// and message such like `MsgRequestVote` should be handle by `Candidate`
    fn advance(&mut self, rd: Ready) -> LightReady;

    /// This will commit and persist `Ready`
    /// Then generate `LightReady` after that
    fn advance_append(&mut self, rd: Ready) -> LightReady;

    fn advance_append_async(&mut self, rd: Ready);

    /// ## Description
    /// Advance apply and update `raft_log.applied` to latest `since_committed_index` 
    /// then return this value. If current peer is `Leader` and has applied some `conf_change`, 
    /// then generate an empty `EntryConfChange` entry, so that auto `LeaveJoint` will
    /// be take place next time.
    fn advance_apply(&mut self) -> u64;

    /// This action maybe update `raft_log.applied` to given `applied`
    fn advance_apply_to(&mut self, applied: u64);

}

impl<S: Storage> RaftProcess for RaftNode<S> where S: Storage {

    fn has_ready(&self) -> bool {
        let raft_peer = &self.raft;
        if !raft_peer.messages.is_empty() || !self.messages.is_empty() {
            trace!("[ready] has messages to send");
            return true;
        }
        if raft_peer.soft_state() != self.prev_soft_state {
            trace!("[ready] soft state change, ori: {:?} now: {:?}", self.prev_soft_state, raft_peer.soft_state());
            return true;
        }
        if raft_peer.hard_state() != self.prev_hard_state {
            trace!("[ready] hard state changed");
            return true;
        }

        if !raft_peer.read_states.is_empty() {
            trace!("[ready] has some read state");
            return true;
        }

        if !raft_peer.raft_log.unstable_entries().is_empty() {
            trace!("[ready] has some unstable entries need to be persist");
            return true;
        }
        if self.snapshot().map_or(false, |snapshot| !snapshot.is_empty()) {
            trace!("[ready] receive snapshot");
            return true;
        }
        if raft_peer.raft_log.is_entries_stable_since(self.since_committed_index) {
            trace!("[ready] commit normal entries");
            return true;
        }

        false
    }

    fn step(&mut self, message: Message) -> Result<()> {
        if Self::is_local_msg(&message) {
            return Err(Error::StepLocalMsg);
        }
        if self.raft.tracker.get(message.from).is_some() || !Self::is_response(&message) {
            return self.raft.process(message);
        }
        Err(Error::StepPeerNotFound)
    }

    fn get_ready(&mut self) -> Ready {
        let raft_peer = &mut self.raft;
        self.ready_records_number += 1;

        let mut ready = Ready {
            seq: self.ready_records_number,
            ..Default::default()
        };
        let mut ready_record = ReadyRecord {
            seq: self.ready_records_number,
            ..Default::default()
        };

        if self.prev_soft_state.raft_state != RaftRole::Leader && raft_peer.current_raft_role == RaftRole::Leader {
            for record in self.records.drain(..) {
                assert_eq!(record.last_entry, None);
                assert_eq!(record.snapshot, None);
                if !record.messages.is_empty() {
                    self.messages.push(record.messages);
                }
            }
        }

        let soft_state = raft_peer.soft_state();
        if soft_state != self.prev_soft_state {
            ready.soft_state = Some(soft_state);
        }
        let hard_state = raft_peer.hard_state();
        if hard_state != self.prev_hard_state {
            if hard_state.vote != self.prev_hard_state.vote || hard_state.term != self.prev_hard_state.term {
                ready.must_sync = true;
            }
            ready.hard_state = Some(hard_state);
        }

        if !raft_peer.read_states.is_empty() {
            mem::swap(&mut ready.read_states, &mut raft_peer.read_states);
        }

        if let Some(snapshot) = raft_peer.raft_log.unstable_snapshot() {
            // get snapshot from unstable raftlog.
            ready.snapshot = snapshot.clone();
            let snapshot_metadata = ready.snapshot.get_metadata();
            let (snapshot_index, snapshot_term) = (snapshot_metadata.index, snapshot_metadata.term);
            assert!(self.since_committed_index <= snapshot_index);
            self.since_committed_index = snapshot_index;
            assert!(
                !raft_peer.raft_log.is_entries_stable_since(self.since_committed_index),
                "has snapshot but also has committed entries since {}",
                self.since_committed_index
            );
            ready_record.snapshot = Some((snapshot_index, snapshot_term));
            ready.must_sync = true;
        }

        ready.unstable_entries = raft_peer.raft_log.unstable_entries().to_vec();
        if let Some(ent) = ready.unstable_entries.last() {
            ready.must_sync = true;
            ready_record.last_entry = Some((ent.index, ent.term));
        }

        // If current peer is not Leader, then don't give it messages (from raft)
        // only Leader can hold messages in light ready when `get_ready`.
        if !raft_peer.messages.is_empty() && raft_peer.current_raft_role != RaftRole::Leader {
            mem::swap(&mut ready_record.messages, &mut raft_peer.messages);
        }

        ready.light_rd = self.gen_light_ready();
        self.records.push_back(ready_record);
        ready
    }

    fn advance(&mut self, rd: Ready) -> LightReady {
        let applied_index = self.since_committed_index;
        let light_rd = self.advance_append(rd);
        self.advance_apply_to(applied_index);
        light_rd
    }

    fn advance_append(&mut self, rd: Ready) -> LightReady {
        self.commit_ready(rd);
        self.persist_ready(self.ready_records_number);
        
        let mut light_rd = self.gen_light_ready();
        let hard_state = self.raft.hard_state();
        if hard_state.commit > self.prev_hard_state.commit {
            light_rd.committed_index = Some(hard_state.commit);
            self.prev_hard_state.commit = hard_state.commit;
        } else {
            assert!(hard_state.commit == self.prev_hard_state.commit);
            light_rd.committed_index = None;
        }
        assert_eq!(hard_state, self.prev_hard_state, "hard state != prev_hard_state",);
        light_rd
    }

    #[inline]
    fn advance_append_async(&mut self, rd: Ready) {
        self.commit_ready(rd)
    }

    #[inline]
    fn advance_apply(&mut self) -> u64 {
        let applied_index = self.since_committed_index;
        self.commit_apply(applied_index);
        applied_index
    }

    #[inline]
    fn advance_apply_to(&mut self, applied_index: u64) {
        self.commit_apply(applied_index)
    }
}
