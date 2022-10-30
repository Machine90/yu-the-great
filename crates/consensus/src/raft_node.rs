use std::{collections::VecDeque, fmt::Debug, mem};

use crate::{protos::{raft_log_proto::{Entry, HardState, Snapshot}, raft_payload_proto::{Message, MessageType}}, prelude::{DUMMY_ID, DUMMY_INDEX}};

use crate::{errors::*, raft::{SoftState, raft_log::Stable, raft_role::{RaftRole, raft_leader::LeaderRaft}, read_only::ReadState}};
use crate::{config::Config, raft::Raft, storage::Storage};

use self::{status::Status};

pub mod raft_functions;
pub mod raft_process;
pub mod ready;
pub mod status;

/// The status of the snapshot.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum SnapshotStatus {
    /// Represents that the snapshot is finished being created.
    Finish,
    /// Indicates that the snapshot failed to build or is not ready.
    Failure,
}

impl Into<String> for SnapshotStatus {

    #[inline]
    fn into(self) -> String {
        match self {
            Self::Finish => "Finish".to_owned(),
            Self::Failure => "Failure".to_owned(),
        }
    }
}

impl ToString for SnapshotStatus {
    fn to_string(&self) -> String {
        match self {
            Self::Finish => "Finish".to_owned(),
            Self::Failure => "Failure".to_owned(),
        }
    }
}

impl From<String> for SnapshotStatus {
    fn from(case: String) -> Self {
        let case = case.to_uppercase();
        let case = case.as_bytes();
        match case {
            b"FINISH" => Self::Finish,
            b"FAILURE" => Self::Failure,
            _ => Self::Failure
        }
    }
}

pub type Utf8 = Vec<u8>;
impl From<Utf8> for SnapshotStatus {
    fn from(utf8: Utf8) -> Self {
        let case = utf8.as_slice();
        match case {
            b"Finish" => Self::Finish,
            b"Failure" => Self::Failure,
            _ => Self::Failure
        }
    }
}


#[derive(Debug, Default, PartialEq)]
pub struct Ready {
    seq: u64,
    hard_state: Option<HardState>,
    soft_state: Option<SoftState>,
    read_states: Vec<ReadState>,
    /// The entries those store in inflights (memory) not persisted yet
    unstable_entries: Vec<Entry>,
    snapshot: Snapshot,
    light_rd: LightReady,
    must_sync: bool
}

/// LightReady encapsulates the committed_index, committed_entries and
/// messages that are ready to be applied or be sent to other peers.
#[derive(Debug, Default, PartialEq)]
pub struct LightReady {
    /// The latest updated quorum's index (if has some). 
    /// when `raft_log.maybe_commit` success, this value 
    /// become Some(`raft_log.quorum_index`).
    committed_index: Option<u64>,
    committed_entries: Vec<Entry>,
    messages: Vec<Vec<Message>>
}

#[derive(Default, Debug, PartialEq)]
struct ReadyRecord {
    seq: u64,
    last_entry: Option<(u64, u64)>,
    snapshot: Option<(u64, u64)>,
    messages: Vec<Message>,
}

pub struct RaftNode<STORAGE: Storage> {
    pub raft: Raft<STORAGE>,
    ready_records_number: u64,
    prev_soft_state: SoftState,
    prev_hard_state: HardState,
    records: VecDeque<ReadyRecord>,
    /// ## Description
    /// `since_committed_index` is dynamic index value, which could be increased when 
    /// invoking `gen_light_ready` at `get_ready` and `advance_append`, it always use to
    /// mark the stable applied index in the raft group. 
    /// ### Update conditions
    /// * **After commit**: when quorum committed index has been changed, then `gen_light_ready`
    /// will refresh it to last stable entry's index.
    /// * **Recv Snapshot**: when received a snapshot from Leader, then refresh it to `snapshot.index`.
    /// ### Noting
    /// since_committed_index <= min(raft_log.persist, raft_log.quorum_index)
    /// ### From
    /// * config.applied (default to 0 if not be set)
    since_committed_index: u64,
    messages: Vec<Vec<Message>>,
}

impl<STORAGE: Storage> Debug for RaftNode<STORAGE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Peer({:?}): {:?}", self.status().role(), self.raft.id)
    }
}

/// Facade of the raft
impl<STORAGE: Storage> RaftNode<STORAGE> {
    pub fn new(store: STORAGE, conf: Config) -> Result<Self> {
        assert_ne!(conf.id, 0, "conf.id must not be zero");
        let mut node = RaftNode {
            raft: Raft::new(store, conf)?,
            ready_records_number: 0,
            prev_soft_state: Default::default(),
            prev_hard_state: Default::default(),
            records: VecDeque::new(),
            since_committed_index: conf.applied,
            messages: Vec::new()
        };
        
        node.prev_soft_state = node.raft.soft_state();
        node.prev_hard_state = node.raft.hard_state();
        Ok(node)
    }

    pub fn set_priority(&mut self, priority: u64) {
        self.raft.priority = priority;
    }

    /// Grabs the snapshot from the raft if available.
    #[inline] pub fn snapshot(&self) -> Option<&Snapshot> {
        self.raft.snapshot_ref()
    }

    /// Status returns the current status of the given group.
    #[inline] pub fn status(&self) -> Status {
        Status::new(&self.raft)
    }

    #[inline] pub fn campaigned(&self) -> bool {
        self.raft.leader_id != DUMMY_ID && self.raft.vote != DUMMY_INDEX
    }

    #[inline] pub fn leader_id(&self) -> u64 {
        self.raft.leader_id
    }

    #[inline] pub fn role(&self) -> RaftRole {
        self.raft.current_raft_role
    }

    #[inline] pub fn prev_ss(&self) -> &SoftState {
        &self.prev_soft_state
    }

    pub fn persist_ready(&mut self, seq: u64) {
        let (mut index, mut term, mut snapshot_index) = (0, 0, 0);
        while let Some(record) = self.records.front() {
            if record.seq > seq {
                break;
            }

            let mut record = self.records.pop_front().unwrap();
            if let Some((idx, _)) = record.snapshot {
                index = 0;
                term = 0;
                snapshot_index = idx;
            }
            if let Some((idx, t)) = record.last_entry {
                index = idx;
                term = t;
            }
            if !record.messages.is_empty() {
                self.messages.push(mem::take(&mut record.messages));
            }
        }

        if snapshot_index != 0 {
            self.raft.persist_snapshot(snapshot_index);
        }
        if index != 0 {
            self.raft.persist_entries(index, term);
        }
    }

    fn commit_ready(&mut self, ready: Ready) {
        if let Some(ss) = ready.soft_state {
            self.prev_soft_state = ss;
        }
        if let Some(hs) = ready.hard_state {
            self.prev_hard_state = hs;
        }

        let ready_record = self.records.back().unwrap();
        assert!(ready_record.seq == ready.seq);

        let raft_peer = &mut self.raft;
        if let Some((index, _)) = ready_record.snapshot {
            // clear snapshot from unstable.
            raft_peer.raft_log.stable_snapshot(index);
        }
        if let Some((index, term)) = ready_record.last_entry {
            raft_peer.raft_log.stable_entries(index, term);
        }
    }

    #[inline]
    fn commit_apply(&mut self, applied_index: u64) {
        self.raft.commit_apply(applied_index)
    }

    #[inline] pub fn store(&self) -> &STORAGE {
        self.raft.store()
    }

    #[inline] pub fn mut_store(&mut self) -> &mut STORAGE {
        self.raft.mut_store()
    }

    /// This action will take  committed entries from raft_log and
    fn gen_light_ready(&mut self) -> LightReady {
        let mut light_rd: LightReady = LightReady::default();
        let raft_peer = &mut self.raft;
        // fetch the committed entries since applied index (to quorum committed or persisted) as committed_entries of lr 
        light_rd.committed_entries = raft_peer
            .raft_log
            .entries_stable_since(self.since_committed_index).unwrap_or_default();
        
        // then try to reduce these committed entries from uncommitted entries 
        raft_peer.reduce_uncommitted_size(&light_rd.committed_entries);

        // then update applied `since_committed_index` to last entry's index of committed entries
        if let Some(last_entry) = light_rd.committed_entries.last() {
            assert!(last_entry.index > self.since_committed_index);
            self.since_committed_index = last_entry.index;
        }

        if !self.messages.is_empty() {
            // take the messages from `record` queue after `persist_ready` in `advance_append` if it has some.
            mem::swap(&mut self.messages, &mut light_rd.messages);
        }

        // take messages from raft as a leader
        if raft_peer.current_raft_role == RaftRole::Leader && !raft_peer.messages.is_empty() {
            light_rd.give_message(&mut raft_peer.messages);
        }
        light_rd
    }

    #[inline] pub fn set_skip_bcast_commit(&mut self, skip: bool) {
        self.raft.skip_bcast_commit = skip;
    }

    #[inline] pub fn set_batch_append(&mut self, batch_append: bool) {
        self.raft.batch_append = batch_append;
    }

    pub fn is_local_msg(message: &Message) -> bool {
        matches!(
            message.msg_type(),
            MessageType::MsgHup
                | MessageType::MsgBeat
                | MessageType::MsgUnreachable
                | MessageType::MsgCheckQuorum
                | MessageType::MsgSnapStatus
        )
    }

    pub fn is_response(message: &Message) -> bool {
        matches!(
            message.msg_type(),
            MessageType::MsgRequestPreVoteResponse
                | MessageType::MsgAppendResponse
                | MessageType::MsgHeartbeatResponse
                | MessageType::MsgRequestVoteResponse
                | MessageType::MsgUnreachable
        )
    }
}

