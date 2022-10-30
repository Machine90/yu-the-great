use std::ops::Range;

use super::{ReadStorage, WriteStorage};
use crate::protos::raft_log_proto::{ConfState, Entry, HardState, Snapshot, SnapshotMetadata};
use common::{
    errors::Result,
    storage::{RaftState, Storage},
};

pub trait GroupStorage: Send + Sync + 'static {
    //=========================================================
    // Basic interfaces required by Raft consensus algorithm
    //=========================================================

    /// get current storage's group id.
    fn get_group_id(&self) -> u32;

    fn group_initial_state(&self, group: u32) -> Result<RaftState>;

    fn group_entries(
        &self,
        group: u32,
        low: u64,
        high: u64,
        limit: Option<u64>,
    ) -> Result<Vec<Entry>>;

    fn group_term(&self, group: u32, index: u64) -> Result<u64>;

    fn group_first_index(&self, group: u32) -> Result<u64>;

    fn group_last_index(&self, group: u32) -> Result<u64>;

    fn group_snapshot(&self, group: u32, index: u64) -> Result<Snapshot>;

    //=========================================================
    // Interfaces implemented in "WriteLock" e.g. RwLockWriteGuard
    //=========================================================

    fn group_append(&self, group: u32, entries: Vec<Entry>) -> Result<usize>;

    fn group_compact(&self, group: u32, to_index: u64) -> Result<()>;

    fn group_set_conf_state(&self, group: u32, initial: ConfState) -> Result<()>;

    fn group_update_hardstate(&self, group: u32, hs: HardState) -> Result<()>;

    fn group_commit_to(&self, group: u32, index: u64) -> Result<()>;

    fn group_apply_snapshot(&self, group: u32, snapshot: SnapshotMetadata) -> Result<()>;

    //=========================================================
    // Interfaces implemented in "ReadLock" e.g. RwLockReadGuard
    //=========================================================
    fn group_scan(&self, group: u32, range: Range<u64>, scanner: &mut dyn FnMut(u32, &Entry));

    fn group_raft_state(&self, group: u32) -> Result<RaftState>;
}

impl Storage for Box<dyn GroupStorage> {
    #[inline]
    fn initial_state(&self) -> Result<RaftState> {
        self.group_initial_state(self.get_group_id())
    }

    #[inline]
    fn entries(&self, low: u64, high: u64, limit: Option<u64>) -> Result<Vec<Entry>> {
        self.group_entries(self.get_group_id(), low, high, limit)
    }

    #[inline]
    fn term(&self, index: u64) -> Result<u64> {
        self.group_term(self.get_group_id(), index)
    }

    #[inline]
    fn first_index(&self) -> Result<u64> {
        self.group_first_index(self.get_group_id())
    }

    #[inline]
    fn last_index(&self) -> Result<u64> {
        self.group_last_index(self.get_group_id())
    }

    #[inline]
    fn snapshot(&self, index: u64) -> Result<Snapshot> {
        self.group_snapshot(self.get_group_id(), index)
    }
}

impl Storage for dyn GroupStorage {
    #[inline]
    fn initial_state(&self) -> Result<RaftState> {
        self.group_initial_state(self.get_group_id())
    }

    #[inline]
    fn entries(&self, low: u64, high: u64, limit: Option<u64>) -> Result<Vec<Entry>> {
        self.group_entries(self.get_group_id(), low, high, limit)
    }

    #[inline]
    fn term(&self, index: u64) -> Result<u64> {
        self.group_term(self.get_group_id(), index)
    }

    #[inline]
    fn first_index(&self) -> Result<u64> {
        self.group_first_index(self.get_group_id())
    }

    #[inline]
    fn last_index(&self) -> Result<u64> {
        self.group_last_index(self.get_group_id())
    }

    #[inline]
    fn snapshot(&self, index: u64) -> Result<Snapshot> {
        self.group_snapshot(self.get_group_id(), index)
    }
}

impl WriteStorage for dyn GroupStorage {
    #[inline]
    fn set_conf_state(&self, initial: ConfState) -> Result<()> {
        self.group_set_conf_state(self.get_group_id(), initial)
    }

    #[inline]
    fn update_hardstate(&self, hs: HardState) -> Result<()> {
        self.group_update_hardstate(self.get_group_id(), hs)
    }

    #[inline]
    fn commit_to(&self, index: u64) -> Result<()> {
        self.group_commit_to(self.get_group_id(), index)
    }

    #[inline]
    fn apply_snapshot(&self, snapshot: SnapshotMetadata) -> Result<()> {
        self.group_apply_snapshot(self.get_group_id(), snapshot)
    }

    #[inline]
    fn append(&self, entries: Vec<Entry>) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }
        if entries[0].index == 0 {
            panic!("don't append the entry with index 0 to store. only accept index from 1");
        }
        self.group_append(self.get_group_id(), entries)
    }

    #[inline]
    fn compact(&self, to_index: u64) -> Result<()> {
        self.group_compact(self.get_group_id(), to_index)
    }
}

impl ReadStorage for dyn GroupStorage {
    #[inline]
    fn raft_state(&self) -> Result<RaftState> {
        self.group_raft_state(self.get_group_id())
    }

    #[inline]
    fn scan<F>(&self, range: Range<u64>, mut scanner: F)
    where
        F: FnMut(u32, &Entry),
    {
        self.group_scan(self.get_group_id(), range, &mut scanner);
    }
}
