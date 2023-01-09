use std::{cmp, sync::Arc};

use common::errors::StorageError;
use consensus::raft::DUMMY_INDEX;

use crate::tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use common::storage::{limit_size, RaftState, Storage};
use crate::protos::raft_log_proto::*;
use crate::torrent::runtime;

// use super::{RaftState, Storage, limit_size};
use crate::{
    ConsensusError::{self},
    RaftResult as Result,
};

#[derive(Clone, Default)]
pub struct MemoryStorage {
    core: Arc<RwLock<MemStorageCore>>,
}

/// The Memory Storage Core instance holds the actual state of the storage struct. To access this
/// value, use the `rl` and `wl` functions on the main MemStorage implementation.
pub struct MemStorageCore {
    raft_state: RaftState,

    // entries[i] has raft log position i+snapshot.get_metadata().index
    entries: Vec<Entry>,
    // Metadata of the last snapshot received.
    applied_snapshot: SnapshotMetadata,
    // If it is true, the next snapshot will return a
    // SnapshotTemporarilyUnavailable error.
    trigger_snap_unavailable: bool,
    applied: u64,
}

impl MemStorageCore {
    /// Get the hard state.
    pub fn hard_state(&self) -> &HardState {
        &self.raft_state.hard_state
    }

    /// Saves the current HardState.
    pub fn update_hardstate(&mut self, hs: HardState) {
        self.raft_state.hard_state = hs;
    }

    pub fn update_commit(&mut self, commit: u64) {
        self.raft_state.hard_state.commit = commit;
    }

    pub fn update_term(&mut self, term: u64) {
        self.raft_state.hard_state.term = term;
    }

    pub fn update_vote(&mut self, vote_for: u64) {
        self.raft_state.hard_state.vote = vote_for;
    }

    /// Commit to an index.
    ///
    /// # Panics
    ///
    /// Panics if there is no such entry in raft logs.
    pub fn commit_to(&mut self, index: u64) -> Result<()> {
        assert!(
            self.has_entry_at(index),
            "commit_to {} but the entry does not exist",
            index
        );

        let diff = (index - self.entries[0].index) as usize;
        self.raft_state.hard_state.commit = index;
        self.raft_state.hard_state.term = self.entries[diff].term;
        Ok(())
    }

    /// Saves the current conf state.
    pub fn set_conf_state(&mut self, cs: ConfState) {
        self.raft_state.conf_state = cs;
    }

    #[inline]
    fn has_entry_at(&self, index: u64) -> bool {
        !self.entries.is_empty() && index >= self.first_index() && index <= self.last_index()
    }

    fn first_index(&self) -> u64 {
        match self.entries.first() {
            Some(entry) => entry.index,
            None => self.applied_snapshot.index + 1,
        }
    }

    fn last_index(&self) -> u64 {
        match self.entries.last() {
            Some(entry) => entry.index,
            None => self.applied_snapshot.index,
        }
    }

    /// Set applied index to `applied_snapshot`, the applied must be 
    /// in range of `[first_index, last_index]`
    pub fn set_applied_index(&mut self, index: u64) -> Result<()> {
        let offset = self.first_index();
        if index < offset {
            return Err(ConsensusError::Store(StorageError::Compacted));
        } else if index > self.last_index() {
            return Err(ConsensusError::Store(StorageError::Unavailable));
        }
        self.applied = index;
        Ok(())
    }

    /// Get applied index from storage
    #[inline]
    pub fn applied_index(&self) -> u64 {
        let mut applied = cmp::max(self.applied_snapshot.index, self.applied).max(self.first_index());
        applied
    }

    #[inline]
    pub fn applied_term(&self) -> u64 {
        self.applied_snapshot.term
    }

    pub fn snapshot(&self) -> Snapshot {
        let mut snapshot = Snapshot::default();

        // We assume all entries whose indexes are less than `hard_state.commit`
        // have been applied, so use the latest commit index to construct the snapshot.
        let meta = snapshot.get_metadata_mut();
        meta.index = self.raft_state.hard_state.commit;
        meta.term = match meta.index.cmp(&self.applied_snapshot.index) {
            cmp::Ordering::Equal => self.applied_snapshot.term,
            cmp::Ordering::Greater => {
                let offset = self.entries[0].index;
                self.entries[(meta.index - offset) as usize].term
            }
            cmp::Ordering::Less => {
                panic!(
                    "commit {} < snapshot_metadata.index {}",
                    meta.index, self.applied_snapshot.index
                );
            }
        };
        meta.conf_state = Some(self.raft_state.conf_state.clone());
        snapshot
    }

    /// Overwrites the contents of this Storage object with those of the given snapshot.
    /// ## Err
    /// Return Err if snapshot index is less than the storage's first index.
    pub fn apply_snapshot(&mut self, snapshot: SnapshotMetadata) -> Result<()> {
        let mut meta = snapshot;
        let index = meta.index;

        if self.first_index() > index {
            return Err(ConsensusError::Store(StorageError::SnapshotOutOfDate));
        }

        self.applied_snapshot = meta.clone();

        self.raft_state.hard_state.term = cmp::max(self.raft_state.hard_state.term, meta.term);
        self.raft_state.hard_state.commit = index;
        self.entries.clear();

        // Update conf states.
        self.raft_state.conf_state = meta.take_conf_state();
        Ok(())
    }

    /// Discards all log entries prior to compact_index.
    /// It is the application's responsibility to not attempt to compact an index
    /// greater than RaftLog.applied.
    /// ## Err
    /// Return Err if `compact_index` is higher than `Storage::last_index(&self) + 1`.
    pub fn compact(&mut self, compact_index: u64) -> Result<()> {
        if compact_index <= self.first_index() {
            // Don't need to treat this case as an error.
            return Ok(());
        }

        if compact_index > self.last_index() + 1 {
            panic!(
                "compact not received raft logs: {}, last index: {}",
                compact_index,
                self.last_index()
            );
        }

        if let Some(entry) = self.entries.first() {
            let offset = compact_index - entry.index;
            self.entries.drain(..offset as usize);
        }
        Ok(())
    }

    /// Append the new entries to storage.
    /// ## Err
    /// Return Err if `ents` contains compacted entries, or there's a gap between `ents` and the last
    /// received entry in the storage.
    pub fn append(&mut self, ents: &[Entry]) -> Result<()> {
        if ents.is_empty() {
            return Ok(());
        }
        if self.first_index() > ents[0].index {
            panic!(
                "overwrite compacted raft logs, compacted: {}, append: {}",
                self.first_index() - 1,
                ents[0].index,
            );
        }
        if self.last_index() + 1 < ents[0].index {
            panic!(
                "raft logs should be continuous, last index: {}, new appended: {}",
                self.last_index(),
                ents[0].index,
            );
        }

        // Remove all entries overwritten by `ents`.
        let diff = ents[0].index - self.first_index();
        self.entries.drain(diff as usize..);
        self.entries.extend_from_slice(&ents);
        Ok(())
    }

    /// Commit to `idx` and set configuration to the given states. Only used for tests.
    pub fn commit_to_and_set_conf_states(&mut self, idx: u64, cs: Option<ConfState>) -> Result<()> {
        self.commit_to(idx)?;
        if let Some(cs) = cs {
            self.raft_state.conf_state = cs;
        }
        Ok(())
    }

    /// Trigger a SnapshotTemporarilyUnavailable error.
    pub fn trigger_snap_unavailable(&mut self) {
        self.trigger_snap_unavailable = true;
    }

    /// Return the entries size.
    #[inline]
    pub fn count(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn get_entries(&self) -> &Vec<Entry> {
        &self.entries
    }
}

impl Default for MemStorageCore {
    fn default() -> MemStorageCore {
        MemStorageCore {
            raft_state: Default::default(),
            entries: vec![],
            // Every time a snapshot is applied to the storage, the metadata will be stored here.
            applied_snapshot: Default::default(),
            // When starting from scratch populate the list with a dummy entry at term zero.
            trigger_snap_unavailable: false,
            applied: 0,
        }
    }
}

impl Storage for MemoryStorage {
    fn term(&self, index: u64) -> Result<u64> {
        let read_lock = self.rlock();
        if index == read_lock.applied_snapshot.index {
            return Ok(read_lock.applied_snapshot.term);
        }
        let offset = read_lock.first_index();
        if index < offset {
            return Err(ConsensusError::Store(StorageError::Compacted));
        } else if index > read_lock.last_index() {
            return Err(ConsensusError::Store(StorageError::Unavailable));
        }
        let term = read_lock.entries[(index - offset) as usize].term;
        Ok(term)
    }

    fn entries(&self, low: u64, high: u64, max_size: Option<u64>) -> Result<Vec<Entry>> {
        // let max_size = max_size.into();
        let read_lock = self.rlock();
        if low < read_lock.first_index() {
            return Err(ConsensusError::Store(StorageError::Compacted));
        }
        if high > read_lock.last_index() + 1 {
            panic!(
                "index out of bound (last: {}, high: {})",
                read_lock.last_index() + 1,
                high
            );
        }
        let offset = read_lock.entries[0].index;
        let lo = (low - offset) as usize;
        let hi = (high - offset) as usize;
        let mut result = read_lock.entries[lo..hi].to_vec();
        limit_size(&mut result, max_size);
        Ok(result)
    }

    fn first_index(&self) -> Result<u64> {
        Ok(self.rlock().first_index())
    }

    fn last_index(&self) -> Result<u64> {
        Ok(self.rlock().last_index())
    }

    fn snapshot(&self, request_index: u64) -> Result<Snapshot> {
        let mut core = self.wlock();
        if core.trigger_snap_unavailable {
            core.trigger_snap_unavailable = false;
            Err(ConsensusError::Store(
                StorageError::SnapshotTemporarilyUnavailable,
            ))
        } else {
            let mut snap = core.snapshot();
            if snap.get_metadata().index < request_index {
                snap.get_metadata_mut().index = request_index;
            }
            Ok(snap)
        }
    }

    fn initial_state(&self) -> Result<RaftState> {
        Ok(self.rlock().raft_state.clone())
    }
}

impl MemoryStorage {
    pub fn new() -> Self {
        let store = MemoryStorage {
            ..Default::default()
        };
        store
    }

    pub fn new_with_conf_state<T>(conf_state: T) -> MemoryStorage
    where
        ConfState: From<T>,
    {
        let store = MemoryStorage::new();
        store.initialize_with_conf_state(conf_state);
        store
    }

    pub fn initialize_with_conf_state<T>(&self, conf_state: T)
    where
        ConfState: From<T>,
    {
        assert!(!self.initial_state().unwrap().initialized());
        let mut core = self.wlock();
        core.set_conf_state(ConfState::from(conf_state));
    }

    pub fn rlock(&self) -> RwLockReadGuard<'_, MemStorageCore> {
        runtime::blocking(self._rlock())
    }

    async fn _rlock(&self) -> RwLockReadGuard<'_, MemStorageCore> {
        self.core.read().await
    }

    pub fn wlock(&self) -> RwLockWriteGuard<'_, MemStorageCore> {
        runtime::blocking(self._wlock())
    }

    async fn _wlock(&self) -> RwLockWriteGuard<'_, MemStorageCore> {
        self.core.write().await
    }
}

impl MemoryStorage {
    #[inline]
    pub fn try_append(&self, entries: &[Entry]) -> Result<usize> {
        self.wlock().append(entries)?;
        Ok(entries.len())
    }

    #[inline]
    pub fn try_reduce_to(&self, compact_index: u64) -> Result<()> {
        self.wlock().compact(compact_index)
    }

    #[inline]
    pub fn try_reduce_all(&self) -> Result<()> {
        let next_index = self.last_index().unwrap() + 1;
        self.wlock().compact(next_index)
    }
}
