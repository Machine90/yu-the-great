use std::mem;

use crate::protos::{raft_log_proto::{HardState, Snapshot}, raft_payload_proto::Message, raft_log_proto::Entry};

use crate::raft::{SoftState, read_only::ReadState};

use super::{LightReady, Ready};

impl LightReady {
    /// The latest updated quorum's commit (if has some). 
    /// when [raft_log](crate::raft::raft_log::RaftLog)
    /// .[maybe_commit(index)](crate::raft::raft_log::RaftLog::maybe_commit()) success, this value 
    /// become Some(`raft_log.quorum_index`).
    /// ### How and When generated
    /// * **When**: only after Peer (leader or follower) advance (or advance_append) [Ready](crate::raft_node::Ready) 
    /// can [LightReady](crate::raft_node::LightReady) generate this value.
    /// * **How**: 
    ///   * *Leader*: update commit success after advance majority follower's accept response.
    ///   * *Follower*: recv high index from Leader's append msg
    /// * **From Where**: This value extract from [HardSate](protos::raft_log_proto::HardState) commit.
    #[inline] pub fn committed_index (&self) -> Option<u64> {
        self.committed_index
    }

    #[inline] pub fn committed_entries(&self) -> &Vec<Entry> {
        &self.committed_entries
    }

    #[inline] pub fn take_committed_entries(&mut self) -> Vec<Entry> {
        mem::take(&mut self.committed_entries)
    }

    #[inline] pub fn get_messages(&self) -> &Vec<Vec<Message>> {
        &self.messages
    }

    #[inline] pub fn take_messages(&mut self) -> Vec<Vec<Message>> {
        mem::take(&mut self.messages)
    }

    /// This method will take all messages from message_box
    /// and give all these messages to `LightReady`. <br/>
    /// Noting that: this action will clear the messages of `raft.messages`
    #[inline] pub fn give_message(&mut self, message_box: &mut Vec<Message>) {
        self.messages.push(mem::take(message_box))
    }
}

impl Ready {

    #[inline] pub fn seq(&self) -> u64 { self.seq }

    #[inline] pub fn hard_state_ref(&self) -> Option<&HardState> {
        self.hard_state.as_ref()
    }

    #[inline] pub fn soft_state_ref(&self) -> Option<&SoftState> {
        self.soft_state.as_ref()
    }

    #[inline] pub fn get_unstable_entries(&self) -> &Vec<Entry> {
        &self.unstable_entries
    }

    /// ## Description
    /// Take unstable entries from `raft_log.unstable`
    #[inline] pub fn take_unstable_entries(&mut self) -> Vec<Entry> {
        mem::take(&mut self.unstable_entries)
    }

    #[inline] pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    #[inline] pub fn some_snapshot(&self) -> Option<Snapshot> {
        if self.snapshot.is_empty() {
            return None;
        }
        let snap = self.snapshot.clone();
        return Some(snap);
    }

    #[inline] pub fn is_must_sync(&self) -> bool {
        self.must_sync
    }

    #[inline] pub fn get_read_states(&self) -> &Vec<ReadState> {
        &self.read_states
    }

    #[inline] pub fn take_read_states(&mut self) -> Vec<ReadState> {
        mem::take(&mut self.read_states)
    }

    #[inline] pub fn light_ready(&self) -> &LightReady {
        &self.light_rd
    }

    /// The latest updated quorum's commit (if has some). 
    /// when [raft_log](crate::raft::raft_log::RaftLog)
    /// .[maybe_commit(index)](crate::raft::raft_log::RaftLog::maybe_commit()) success, this value 
    /// become Some(`raft_log.quorum_index`).
    /// ### How and When generated
    /// * **When**: only after Peer (leader or follower) advance (or advance_append) [Ready](crate::raft_node::Ready) 
    /// can [LightReady](crate::raft_node::LightReady) generate this value.
    /// * **How**: 
    ///   * *Leader*: update commit success after advance majority follower's accept response.
    ///   * *Follower*: recv high index from Leader's append (or heartbeat) msg
    #[inline] pub fn committed_index(&self) -> Option<u64> {
        self.light_rd.committed_index()
    }

    /// Get committed entries from ready (if exists).
    /// The committed entries defined to which index in range from
    /// `since_committed_index` to `min(raft_log.persist, raft_log.quorum_commtted)`
    /// ### Generated
    /// * **When**: current peer's quorum commit has been changed, then entries will be 
    /// fetch from storage and set to light_ready when `get_ready()`
    #[inline] pub fn committed_entries(&self) -> &Vec<Entry> {
        self.light_rd.committed_entries()
    }

    #[inline] pub fn take_committed_entries(&mut self) -> Vec<Entry> {
        self.light_rd.take_committed_entries()
    }

    #[inline] pub fn get_messages(&self) -> &Vec<Vec<Message>> {
        self.light_rd.get_messages()
    }

    #[inline] pub fn take_messages(&mut self) -> Vec<Vec<Message>> {
        self.light_rd.take_messages()
    }
}