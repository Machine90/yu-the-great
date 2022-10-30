use std::usize;

use crate::protos::raft_log_proto::{Entry, Snapshot};
use super::{Stable};

/// The unstable.buffer[i] has raft log position i + unstable.offset.
/// Note that unstable.offset may be less than the highest log
/// position in storage; this means that the next write to storage
/// might need to truncate the log before persisting unstable.entries.
#[derive(Debug)]
pub struct Unstable {
    pub snapshot: Option<Snapshot>,
    pub buffer: Vec<Entry>,
    pub offset: u64,
}

impl Unstable {

    pub fn new(offset: u64) -> Self {
        Unstable {
            offset,
            snapshot: None,
            buffer: vec![]
        }
    }

    pub fn restore(&mut self, snapshot: Snapshot) {
        self.buffer.clear();
        self.offset = snapshot.get_metadata().index + 1;
        self.snapshot = Some(snapshot);
    }

    pub fn maybe_first_index(&self) -> Option<u64> {
        self.snapshot
            .as_ref()
            .map(|snap| snap.get_metadata().index + 1)
    }

    pub fn maybe_last_index(&self) -> Option<u64> {
        match self.buffer.len() {
            0 => self.snapshot.as_ref().map(|snap| snap.get_metadata().index),
            len => Some(self.offset + len as u64 - 1)
        }
    }

    pub fn slice(&self, lo: u64, hi: u64) -> &[Entry] {
        let (lo, hi) = self.deal_with_boundary(lo, hi);
        let offset = self.offset as usize;
        &self.buffer[lo - offset..hi - offset]
    }

    pub fn maybe_term(&self, index: u64) -> Option<u64> {
        if index < self.offset {
            let snapshot = self.snapshot.as_ref()?;
            let meta = snapshot.get_metadata();
            if index == meta.index {
                return Some(meta.term);
            } else {
                return None;
            }
        } else {
            return self.maybe_last_index().and_then(|last| {
                if last < index {
                    return None;
                }
                Some(self.buffer[(index - self.offset) as usize].term)
            });
        }
    }

    /// Stash entries to unstable buffer
    /// ## Notes 
    /// There has 3 cases when stashing buffer, see: <br/>
    /// * ***Case 1:*** <br/>
    ///   when buffered is {3,4,5} and entries is {1,2,3,4} (or is {1,2}) <br/>
    ///   then just replace buffer to {1,2,3,4} (or to {1,2}), offset is 1 <br/>
    /// * ***Case 2:*** <br/>
    ///   when buffered is {3,4,5} and entries is {6,7,8} <br/>
    ///   then just append entries to end of buffer, {3,4,5,6,7,8}, offset still 3 <br/>
    /// * ***Case 3:*** <br/>
    ///   Then Keep the origin buffered which index <= after and replace others with new entries <br/>
    ///   Assumes buffered is {3,4,5,6} and entries in cases below: </br>
    ///   1. if {4,6,7} then {3,4,6,7}
    ///   2. if {8,9,10} then {3,4,5,6,8,9,10}
    ///   offset still be 3
    pub fn stash(&mut self, entries: &[Entry]) {
        let after = entries[0].index;
        if after == self.offset + self.buffer.len() as u64 {
            // step in case 2
        } else if after <= self.offset {
            // step in case 1
            self.offset = after;
            self.buffer.clear();
        } else {
            // step in case 3
            let offset = self.offset;
            self.deal_with_boundary(offset, after);
            self.buffer.truncate((after - offset) as usize);
        }
        self.buffer.extend_from_slice(entries);
    }

    pub fn deal_with_boundary(&self, lo: u64, hi: u64) -> (usize, usize) {
        // TODO, here we can use a policy, e.g. we change the hi to "real high" (of bubffer) when hi larger than buffer's high
        // now we throw out error when out of boundary
        if lo > hi {
            panic!("invalid range of low: {} high: {}", lo, hi);
        }

        let buffer_high = self.offset + self.buffer.len() as u64;
        if lo < self.offset || hi > buffer_high {
            panic!(
                "out of range, input: [low({}), .. hi({})] actual: [offset({}) .. high({})], ",
                lo, hi, self.offset, buffer_high
            );
        }
        (lo as usize, hi as usize)
    }
}


impl Stable for Unstable {

    /// To stable (actually, it's clear) the snapshot in the unstable storage with expected index.
    /// ### Params
    /// * ***expected_index***: this index will be used to match the index of snapshot, only if it's matched will be approve
    fn stable_snapshot(&mut self, expected_index: u64) {
        if let Some(snapshot) = self.snapshot.as_ref() {
            let snapshot_index = snapshot.get_metadata().index;
            if snapshot_index == expected_index {
                // if index of need stable snapshot is expected, the clear the snapshot
                self.snapshot = None;
                return;
            }
            panic!("unstable.snapshot has has different index: {} from expected: {}", snapshot_index, expected_index);
        } else {
            panic!("unstable snapshot is None, require one snapshot of index: {}", expected_index);
        }
    }

    fn stable_entries(&mut self, expected_index: u64, expected_term: u64) {
        assert!(self.snapshot.is_none());
        if let Some(last_entry) = self.buffer.last() {
            if last_entry.index != expected_index || last_entry.term != expected_term {
                panic!(
                    "the last one of unstable.buffer has different index {} and term {}, expect {} {}",
                    last_entry.index, last_entry.term, expected_index, expected_term
                );
            }
            self.offset = last_entry.index + 1;
            self.buffer.clear();
        } else {
            panic!(
                "unstable.buffer is empty, suppose there has at least one entry in buffer at term({}) index({})",
                expected_index, expected_term
            );
        }
    }
}

