use std::ops::{Range};

use common::protos::raft_log_proto::SnapshotMetadata;
use common::storage::{RaftState, Storage};
use crate::protos::raft_log_proto::{ConfState, Entry, Snapshot, HardState};

use crate::{
    GroupID,
    storage::{group_storage::GroupStorage},
    RaftResult,
};

use super::storage_impl::MemoryStorage;

#[derive(Clone)]
pub struct PeerMemStore {
    pub current: GroupID,
    pub db: MemoryStorage,
}

impl PeerMemStore {
    pub fn new(group: GroupID, db: MemoryStorage) -> Self {
        Self {
            current: group,
            db,
        }
    }
}

impl GroupStorage for PeerMemStore {
    #[inline]
    fn get_group_id(&self) -> GroupID {
        self.current
    }

    #[inline]
    fn group_initial_state(&self, group: GroupID) -> RaftResult<RaftState> {
        self.db.initial_state()
    }

    #[inline]
    fn group_entries(
        &self,
        group: u32,
        low: u64,
        high: u64,
        limit: Option<u64>,
    ) -> RaftResult<Vec<Entry>> {
        self.db.entries(low, high, limit)
    }

    #[inline]
    fn group_term(&self, group: u32, index: u64) -> RaftResult<u64> {
        self.db.term(index)
    }

    #[inline]
    fn group_first_index(&self, group: u32) -> RaftResult<u64> {
        self.db.first_index()
    }

    #[inline]
    fn group_last_index(&self, group: u32) -> RaftResult<u64> {
        self.db.last_index()
    }

    #[inline]
    fn group_snapshot(&self, group: u32, index: u64) -> RaftResult<Snapshot> {
        self.db.snapshot(index)
    }

    #[inline]
    fn group_append(&self, group: u32, entries: Vec<Entry>) -> RaftResult<usize> {
        self.db.try_append(&entries[..])
    }

    fn group_scan(&self, group: u32, range: Range<u64>, scanner: &mut dyn FnMut(u32, &Entry)) {
        let Range { start, end } = range;
        for ent in self
            .db
            .rlock()
            .get_entries()
            .iter()
            .filter(|ent| ent.index >= start && ent.index < end)
        {
            scanner(group, ent);
        }
    }

    #[inline]
    fn group_compact(&self, group: u32, to_index: u64) -> crate::RaftResult<()> {
        self.db.try_reduce_to(to_index)
    }

    #[inline]
    fn group_raft_state(&self, group: u32) -> RaftResult<RaftState> {
        self.db.initial_state()
    }

    #[inline]
    fn group_set_conf_state(
        &self,
        group: u32,
        initial: ConfState,
    ) -> RaftResult<()> {
        Ok(self.db.wlock().set_conf_state(initial))
    }

    #[inline]
    fn group_update_hardstate(
        &self,
        group: u32,
        hs: HardState,
    ) -> RaftResult<()> {
        Ok(self.db.wlock().update_hardstate(hs))
    }

    #[inline]
    fn group_commit_to(&self, group: u32, index: u64) -> crate::RaftResult<()> {
        self.db.wlock().commit_to(index)
    }

    #[inline]
    fn group_apply_snapshot(&self, group: u32, snapshot: SnapshotMetadata) -> crate::RaftResult<()> {
        self.db.wlock().apply_snapshot(snapshot)
    }

    #[inline]
    fn group_update_applid(&self, _: u32, index: u64) -> crate::RaftResult<()> {
        self.db.wlock().set_applied_index(index)
    }

    #[inline]
    fn group_applid(&self, _: u32) -> Option<u64> {
        Some(self.db.rlock().applied_index())
    }
}
