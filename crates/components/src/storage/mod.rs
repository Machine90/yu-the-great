pub mod group_storage;
#[cfg(feature = "multi")]
pub mod multi_storage;

use crate::protos::raft_log_proto::{ConfState, Entry, HardState, SnapshotMetadata};
use common::{
    errors::Result,
    storage::{RaftState, Storage},
};
use std::ops::Range;

pub trait ReadStorage: Storage {

    fn raft_state(&self) -> Result<RaftState>;

    fn get_applid(&self) -> Option<u64>;

    /// Scan entries those in given index range
    fn scan<F>(&self, range: Range<u64>, scanner: F)
    where
        F: FnMut(u32, &Entry);
}

pub trait WriteStorage {

    fn set_conf_state(&self, initial: ConfState) -> Result<()>;

    fn update_applied(&self, applied: u64);

    /// commit in hardstate should be update independency
    fn update_commit(&self, commit: u64) -> Result<()> {
        let mut hs: HardState = HardState::default();
        hs.commit = commit;
        self.update_hardstate(hs)
    }

    /// Update hardstate of specific raft, this method will be
    /// invoked if detect some hardstate changed includes vote,
    /// term and commit.
    fn update_hardstate(&self, hs: HardState) -> Result<()>;

    /// Update commit only
    fn commit_to(&self, index: u64) -> Result<()>;

    fn apply_snapshot(&self, snapshot: SnapshotMetadata) -> Result<()>;

    /// Append entries in order to Raft storage. These entries will be persisted.
    /// Noting, never append an entry with index 0 to storage.
    fn append(&self, entries: Vec<Entry>) -> Result<usize>;

    fn compact(&self, to_index: u64) -> Result<()>;
}
