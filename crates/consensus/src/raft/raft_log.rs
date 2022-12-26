
use std::{cmp::{max, min}, usize};

use crate::protos::raft_log_proto::{Entry, EntryType, Snapshot};

use self::unstable::Unstable;
use crate::{storage::{Storage, limit_size}};

mod unstable;
use crate::{errors::*, debug};

use super::{DUMMY_INDEX, DUMMY_TERM};


/// The implementation of raft log.
/// Entries in current raft: 
/// ```rust
/// Store (stable)                       Buffer (unstable) 
/// ------------------------------------|--------------------------- 
///  ____                        ____       _____              ____
/// |__1_| -->  ... ...    -->  |__x_|  |  |_x+1_|  ...  -->  |__m_|  
/// ------------------------------|-----|----|---------------------- 
///                               | index    |
///                            persisted   offset
///
///                       applied =< min(persisted, quorum_committed)
///
///                                       quorum_committed
/// Entries in quorum (other nodes):          |
/// Store (stable)                            |    Buffer (unstable) 
/// ------------------------------------------|---|--------------------------- 
///  ____                      ____         ____      _____              ____
/// |__1_| -->  ... ...  -->  |__x_|  -->  |__y_| |  |_y+1_|  ...  -->  |__n_|  
/// ----------------------------------------------|--------------------------- 
///```
pub struct RaftLog<S: Storage> {
    /// Contains all stable entries since the last snapshot
    pub store: S,

    /// Contains all unstable entries and snapshot in memory, 
    /// All these unstable data will be flush to stable store
    pub unstable: Unstable,

    /// The highest log position that is known to be in stable storage
    /// on a quorum of nodes.
    ///
    /// Invariant: applied <= quorum_committed
    pub quorum_committed: u64,

    /// The highest log position that the application has been instructed
    /// to apply to its state machine.
    ///
    /// Invariant: applied <= min(quorum_committed, persisted)
    applied: u64,

    /// The highest log position that is known to be persisted in stable
    /// storage. It's used for limiting the upper bound of quorum_committed and
    /// persisted entries.
    ///
    /// Invariant: persisted < unstable.offset && applied <= persisted
    persisted: u64,
}

/// This enum represent the work state of current raft log
pub enum LogState {
    ConfChangePending,
    IDLE,
}

impl<S> RaftLog<S>
where
    S: Storage,
{
    /// Initialize the RaftLog with store.first_index and store.last_index
    pub fn new(store: S) -> RaftLog<S> {
        let first_idx = store.first_index().unwrap();
        let last_index = store.last_index().unwrap();

        RaftLog {
            store,
            quorum_committed: Default::default(),
            applied: first_idx - 1, // initial
            persisted: last_index, // initial
            unstable: Unstable::new(last_index + 1),
        }
    }

    pub fn restore(&mut self, snapshot: Snapshot) {
        debug!(
            "log [{log}] starts to restore snapshot [index: {snapshot_index}, term: {snapshot_term}]", 
            log = self.to_string(),
            snapshot_index = snapshot.get_metadata().index,
            snapshot_term = snapshot.get_metadata().term,
        );
        let restore_index = snapshot.get_metadata().index;
        assert!(restore_index >= self.quorum_committed, "{} < {}", restore_index, self.quorum_committed);
        // If `persisted` is greater than `quorum_committed`, reset it to `quorum_committed`.
        // It's because only the persisted entries whose index are less than `commited` can be
        // considered the same as the data from snapshot.
        // Although there may be some persisted entries with greater index are also quorum_committed,
        // we can not judge them nor do we care about them because these entries can not be applied
        // thus the invariant which is `applied` <= min(`persisted`, `quorum_committed`) is satisfied.
        if self.quorum_committed < self.persisted {
            self.persisted = self.quorum_committed;
        }
        self.quorum_committed = restore_index;
        self.unstable.restore(snapshot);
    }

    #[inline] pub fn get_persisted(&self) -> u64 {
        self.persisted // getter
    }

    #[inline] pub fn get_applied(&self) -> u64 {
        self.applied // getter
    }

    /// Grab a read-only reference to the underlying storage.
    #[inline]
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Grab a mutable reference to the underlying storage.
    #[inline]
    pub fn mut_store(&mut self) -> &mut S {
        &mut self.store
    }

    pub fn log_state(&self, from_index: Option<u64>) -> LogState {
        let from_index = match from_index {
            Some(first_index) => first_index,
            None => {
                let stored_offset = match self.unstable.maybe_first_index() {
                    Some(i) => i,             // if exists index in the unstable storage, then use it.
                    None => self.applied + 1, // otherwise use next one of applied index
                };
                stored_offset
            }
        };

        let entries = self
            .entries(from_index, self.quorum_committed + 1, None)
            .unwrap_or_else(|err| {
                panic!(
                    "unexpected error getting unapplied entries [{}, {}): {:?}",
                    from_index,
                    self.quorum_committed + 1,
                    err
                )
            });
        let pending_conf_num = entries
            .iter()
            .filter(|ent| ent.entry_type() == EntryType::EntryConfChange)
            .count();

        if pending_conf_num != 0 {
            // still exists some conf changes need be complete when in this case
            return LogState::ConfChangePending;
        }
        return LogState::IDLE;
    }

    /// Determines if the given (index, term) log is more up-to-date
    /// by comparing the index and term of the last entry in the existing logs.
    /// If the logs have last entry with different terms, then the log with the
    /// later term is more up-to-date. If the logs end with the same term, then
    /// whichever log has the larger last_index is more up-to-date. If the logs are
    /// the same, the given log is up-to-date.
    pub fn is_up_to_date(&self, index: u64, term: u64) -> bool {
        term > self.last_term() || (term == self.last_term() && index >= self.last_index().unwrap())
    }

    #[inline]
    pub fn unstable(&self) -> &Unstable {
        &self.unstable
    }

    #[inline]
    pub fn unstable_entries(&self) -> &[Entry] {
        &self.unstable.buffer
    }

    #[inline]
    pub fn unstable_snapshot(&self) -> Option<&Snapshot> {
        self.unstable.snapshot.as_ref()
    }

    #[inline]
    pub fn update_applied_index(&mut self, applied: u64) {
        self.set_applied(applied);
    }

    fn set_applied(&mut self, index: u64) {
        if index == 0 { return; }
        if index > min(self.quorum_committed, self.persisted) || index < self.applied {
            panic!(
                "applied: {} should always less than quorum_committed: {} and persisted: {}, and larger than origin applied({})", 
                index, self.quorum_committed, self.persisted, self.applied
            );
        }
        self.applied = index; // setter
    }
 
    /// Try to find the `conflict index` on given `term`, the `conflict index` less or equal to 
    /// given `index`.
    pub fn find_conflict_by_term(&self, mut index: u64, term: u64) -> (u64, Option<u64>) {
        let last_index = self.last_index().unwrap();
        if index > last_index {
            // conflict index should not be larger than last index (in both unstable and stable)
            return (index, None);
        }
        while index > 0 {
            match self.term(index) {
                Ok(current_term) => {
                    if current_term <= term {
                        // if current_term <= input term and index is equal, then current term is conflict
                        return (index, Some(current_term));
                    }
                    // advance to previous index
                    index -= 1;
                },
                Err(_err) => {
                    // there has not index matched expected conflict index
                    return (index, None);
                }
            }
        }
        (index, None)
    }

    /// Finds the first index of the conflict.
    /// ## Params 
    /// * entries: the entries prepare to check
    /// ## Returns
    /// * u64: INVALID_INDEX if there has not conflict betwee passed in entries and storage,
    /// otherwise return conflict_index
    /// ## Example
    /// Assume there exists (index, term) entries in storage(both stable unstable), it's 
    /// looks like: <br/> 
    /// `[(1,1), (2,1), (3,1), (4,2), (5,2)]`
    ///
    /// The entries passed in looks like: <br/>
    /// `[(2,1), (3,2), (4,2), (5,3), (6,3)]`
    /// 
    /// Then return will be 3, and the first conflict entry if `(3,2)`
    /// 
    /// ## Description
    /// It returns the first index of conflicting entries between the existing
    /// entries and the given entries, if there are any.
    ///
    /// If there are no conflicting entries, and the existing entries contain
    /// all the given entries, zero will be returned.
    ///
    /// If there are no conflicting entries, but the given entries contains new
    /// entries, the index of the first new entry will be returned.
    ///
    /// An entry is considered to be conflicting if it has the same index but
    /// a different term.
    ///
    /// The first entry MUST have an index equal to the argument 'from'.
    /// The index of the given entries MUST be continuously increasing.
    pub fn find_conflict(&mut self, entries: &[Entry]) -> u64 {
        for entry in entries {
            if !self.match_term(entry.index, entry.term) {
                // if the entry in same index with difference term, it means conflict occurred
                let conflict_index = entry.index;
                if conflict_index <= self.last_index().unwrap() {
                    debug!(
                        "found conflict at index {index}", index = conflict_index; 
                        "stored term" => self.term(entry.index).unwrap_or(0),
                        "conflict term in append entry" => entry.term
                    );
                }
                return conflict_index;
            }
        }
        DUMMY_INDEX
    }

    pub fn commit_to(&mut self, index: u64) {
        if self.quorum_committed >= index {
            // commit index should larger than committed value
            return;
        }
        if self.last_index().unwrap() < index {
            // committed index should never larger than last index that stored in buffer (or stable storage)
            // means that the committed index always has been stored(exists) in the storage
            panic!("prepared commit index is out of range from inflights entries");
        }
        debug!("update committed to index({:?}), old committed: {:?}", index, self.quorum_committed);
        // update committed index if all conditions matched
        self.quorum_committed = index; // update quorum commit
    }

    pub fn maybe_commit(&mut self, index: u64, term: u64) -> bool {
        let tried = if index > self.quorum_committed && self.match_term(index, term) {
            debug!("committing index: {index}");
            self.commit_to(index);
            true
        } else {
            false
        };
        tried
    }

    /// Grab the commited entry info if exists (in stable & unstable)
    /// ## Returns
    /// * (index, term) committed index and matched term (of committed)
    pub fn commit_info(&self) -> (u64, u64) {
        match self.term(self.quorum_committed) {
            Ok(matched_term) => (self.quorum_committed, matched_term),
            Err(e) => {
                panic!(
                    "last committed entry at {} is missing: {:?}",
                    self.quorum_committed,
                    e
                )
            }
        }
    }

    /// Attempt to persist index and term (different from etcd)
    pub fn maybe_persist(&mut self, index: u64, term: u64) -> bool {
        // It's possible that the term check can be passed but index is greater
        // than or equal to the first_update_index in some corner cases.
        // For example, there are 5 nodes, A B C D E.
        // 1. A is leader and it proposes some raft logs but only B receives these logs.
        // 2. B gets the Ready and the logs are persisted asynchronously.
        // 2. A crashes and C becomes leader after getting the vote from D and E.
        // 3. C proposes some raft logs and B receives these logs.
        // 4. C crashes and A restarts and becomes leader again after getting the vote from D and E.
        // 5. B receives the logs from A which are the same to the ones from step 1.
        // 6. The logs from Ready has been persisted on B so it calls on_persist_ready and comes to here.
        //
        // We solve this problem by not forwarding the persisted index. It's pretty intuitive
        // because the first_update_index means there are snapshot or some entries whose indexes
        // are greater than or equal to the first_update_index have not been persisted yet.
        let first_update_index = match self.unstable.snapshot.as_ref() {
            Some(snapshot) => snapshot.get_metadata().index,
            None => self.unstable.offset
        };
        if index > self.persisted 
            && index < first_update_index 
            && self.store.term(index).map_or(false, |t| t == term) 
        {
            self.persisted = index;
            return true;
        }
        false
    }

    pub fn maybe_persist_snapshot(&mut self, snapshot_index: u64) -> bool {
        if snapshot_index <= self.persisted {
            return false;
        }
        if snapshot_index > self.quorum_committed {
            // snapshot index should never larger than committed index in the quorum
            panic!("[consensus] not allow to persist a snapshot which index larger than quorum's index");
        }
        if snapshot_index >= self.unstable.offset {
            panic!("[consensus] not allow to persist a snapshot with index that ")
        }
        self.persisted = snapshot_index;
        true
    }

    /// Find specific index and term in both stable and unstable storage, true if matched
    pub fn match_term(&self, index: u64, term: u64) -> bool {
        self.term(index).map(|found_term| { found_term == term }).unwrap_or(false)
    }

    #[inline]
    pub fn last_term(&self) -> u64 {
        match self.term(self.last_index().unwrap()) {
            Ok(last_term) => last_term,
            Err(e) => panic!("unexpected error when getting the last term, refer to: {:?}", e)
        }
    }

    pub fn append(&mut self, entries: &[Entry]) -> u64 {
        crate::trace!(
            "Entries being appended to unstable list";
            "ents" => ?entries,
        );

        if entries.is_empty() {
            return self.last_index().unwrap();
        }

        let next_idx = entries[0].index - 1;
        if next_idx < self.quorum_committed {
            // the next entry should not be less than committed index (stable value) in the quorum
            // appended entries should only appended to unstable
            panic!("the next entry's index should never be less than committed index (stable value) in the quorum")
        }
        self.unstable.stash(entries);
        self.last_index().unwrap()
    }

    /// Try append entries without conflict to unstable storage and commit, parts of conflicted will be dropped.
    /// ## Params
    /// * index:        the index received from LeaderRaft message
    /// * term:         term received from LeaderRaft msg
    /// * committed:    the leader raft may tell us the actual committed in the quorum
    /// * entries:      the entries that leader raft want append to current raft
    /// ## Returns (None if append failed)
    /// * conflict_index:   there maybe exists conflict entry beween passed entries and stored entries
    /// * committed:        the actual committed value
    /// ## Exceptions
    pub fn maybe_append(&mut self, m_index: u64, m_term: u64, m_committed: u64, m_entries: &[Entry]) -> Option<(u64, u64)> {
        if !self.match_term(m_index, m_term) {
            return None;
        }

        let conflict_index = self.find_conflict(m_entries);
        if conflict_index == DUMMY_INDEX { // there has not conflict between entries and stored
        } else if conflict_index <= self.quorum_committed {
            // when in this case, it's means there has conflict between committed entries and entries prepare to append
            // this is an unexpected case in raft.
            panic!("entry {} conflict with committed entry {}", conflict_index, self.quorum_committed);
        } else {
            // otherwise append these entries (not conflict part) to unstable
            let append_from = (conflict_index - (m_index + 1)) as usize;
            self.append(&m_entries[append_from..]);
            if self.persisted > conflict_index - 1 {
                self.persisted = conflict_index - 1;
            }
        }
        let calculated_committed = m_index + m_entries.len() as u64;
        self.commit_to(min(m_committed, calculated_committed));
        crate::trace!("last_append_index: {:?}", calculated_committed);
        Some((conflict_index, calculated_committed))
    }

    pub fn entries_all(&self) -> Vec<Entry> {
        let start = self.first_index().unwrap();
        match self.entries_remain(start, None) {
            Ok(entries) => entries,
            Err(err) => {
                if err == Error::Store(StorageError::Compacted) {
                    return self.entries_all();
                }
                panic!("log as fatal")
            },
        }
    }

    pub fn entries_remain(&self, from_index: u64, limit: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        let end = self.last_index().unwrap();
        if end < from_index {
            return Ok(Vec::new());
        }
        let limit = limit.into();
        self.entries(from_index, end + 1, limit)
    }

    /// Fetch all committed (or persisted) entries from `stable` storage
    /// since applied entry's index
    pub fn entries_stable(&self) -> Option<Vec<Entry>> {
        self.entries_stable_since(self.applied)
    }

    /// Fetch all committed (or persisted) entries from `stable` storage
    /// from given start index.
    /// ## Params
    /// * from_index: the index passed in, should not be larger than `quorum_committed` and `persisted`
    pub fn entries_stable_since(&self, from_index: u64) -> Option<Vec<Entry>> {
        let offset = max(from_index + 1, self.first_index().unwrap());
        let high = min(self.quorum_committed, self.persisted) + 1;
        if high > offset {
            match self.entries(offset, high, None) {
                Ok(entries) => {
                    return Some(entries);
                },
                Err(err) => panic!("{}", err)
            }
        }
        None
    }

    pub fn is_entries_stable(&self) -> bool {
        self.is_entries_stable_since(self.applied)
    }

    pub fn is_entries_stable_since(&self, from_index: u64) -> bool {
        let offset = max(from_index + 1, self.first_index().unwrap());
        let high = min(self.quorum_committed, self.persisted) + 1;
        high > offset
    }

    fn validate_boundary(&self, l: u64, h: u64) -> Option<Error> {
        if l > h {
            panic!("invalid slice {} > {}", l, h);
        }
        let first_index = self.first_index().unwrap();
        if l < first_index {
            return Some(Error::Store(StorageError::Compacted));
        } 
        let last_index = self.last_index().unwrap();
        let len = last_index + 1 - first_index;
        if l < first_index || h > first_index + len {
            panic!(
                "slice[{},{}] out of bound[{},{}]",
                l,
                h,
                first_index,
                last_index
            )
        }
        None
    }
}

/// Implement basic storage features for RaftLog
/// This implement combine unstable and 'stable'(abstract) storage
impl<S> Storage for RaftLog<S>
where
    S: Storage,
{
    /// Get the entries in range of low to high from both storage (of raft) and unstable buffer
    fn entries(
        &self,
        low: u64,
        high: u64,
        limit: Option<u64>,
    ) -> crate::errors::Result<Vec<Entry>> {
        let mut result: Vec<Entry> = vec![];
        if low == high {
            return Ok(result);
        }
        if let Some(err) = self.validate_boundary(
            low,
            high,
        ) {
            return Err(err);
        }

        let limit = limit.into();
        if low < self.unstable.offset {
            // this means that part of queried entries should be fetched from storage
            let unstable_high = min(self.unstable.offset, high);
            match self.store.entries(low, unstable_high, limit) {
                Ok(entries) => {
                    // the queried entries only stored in stable storage, not in unstable buffer
                    result = entries;
                    if (result.len() as u64) < (unstable_high - low) {
                        return Ok(result);
                    }
                }
                Err(err) => match err {
                    Error::Store(StorageError::Compacted) => return Err(err),
                    Error::Store(StorageError::Unavailable) => {
                        panic!("entries[{}:{}] is unavailable from storage",
                        low,
                        unstable_high,)
                    }
                    _ => {
                        panic!("unexpect error: {:?}", err);
                    }
                },
            }
        }

        // still remain some quired entries stored in buffer
        if high > self.unstable.offset {
            let offset = self.unstable.offset;
            let unstable = self.unstable.slice(max(offset, low), high);
            result.extend_from_slice(unstable); 
        }
        limit_size(&mut result, limit);
        Ok(result)
    }

    /// Grab specific entry's term via index from unstable (if exists) or storage
    fn term(&self, index: u64) -> crate::errors::Result<u64> {
        let dummy_idx = self.first_index().unwrap() - 1;
        if index < dummy_idx || index > self.last_index().unwrap() {
            return Ok(DUMMY_TERM);
        }
        match self.unstable.maybe_term(index) {
            Some(term) => Ok(term),
            _ => self.store.term(index).map_err(|err| {
                match err {
                    Error::Store(StorageError::Compacted)
                    | Error::Store(StorageError::Unavailable) => {}
                    _ => panic!("unexpected error: {:?}", err),
                }
                err
            }),
        }
    }

    /// Grab first entry's index from unstable (if exists) or storage
    fn first_index(&self) -> crate::errors::Result<u64> {
        match self.unstable.maybe_first_index() {
            Some(index) => Ok(index),
            _ => Ok(self.store.first_index().unwrap()),
        }
    }

    /// Grab last entry's index from unstable (if exists) or storage
    fn last_index(&self) -> crate::errors::Result<u64> {
        match self.unstable.maybe_last_index() {
            Some(index) => Ok(index),
            None => Ok(self.store.last_index().unwrap()),
        }
    }

    fn snapshot(&self, index: u64) -> crate::errors::Result<Snapshot> {
        if let Some(snapshot) = self.unstable.snapshot.as_ref() {
            if index <= snapshot.get_metadata().index {
                return Ok(snapshot.clone());
            }
        }
        return self.store.snapshot(index);
    }

    fn initial_state(&self) -> Result<crate::storage::RaftState> {
        self.store.initial_state()
    }
}



/// Stable trait for raft log and unstable, which make unstable store (buffer, snapshot etc...) to stable.
pub trait Stable {

    fn stable_snapshot(&mut self, expected_index: u64);

    fn stable_entries(&mut self, expected_index: u64, expected_term: u64);
}

impl<S> Stable for RaftLog<S>
where
    S: Storage,
{
    fn stable_snapshot(&mut self, expected_index: u64) {
        self.unstable.stable_snapshot(expected_index);
    }

    fn stable_entries(&mut self, expected_index: u64, expected_term: u64) {
        self.unstable.stable_entries(expected_index, expected_term);
    }
}

impl<S> ToString for RaftLog<S> where S: Storage {
    fn to_string(&self) -> String {
        format!(
            "quorum_committed={}, persisted={}, applied={}, unstable.offset={}, unstable.buffer.len()={}",
            self.quorum_committed,
            self.persisted,
            self.applied,
            self.unstable.offset,
            self.unstable.buffer.len()
        )
    }
}