
use crate::prost::Message;
use crate::prost::bytes::BytesMut;
use crate::protos::{raft_conf_proto::BatchConfChange, raft_payload_proto::{Message as RaftMessage, MessageType}, raft_log_proto::{Entry, EntryType, ConfState}};

use crate::{raft::raft_role::{raft_follower::FollowerRaft, raft_leader::LeaderRaft}, storage::Storage};

use super::{RaftNode, SnapshotStatus};
use crate::raft::raft_role::RaftRole::*;
use crate::errors::*;

/// Interface of all explored Raft's method.
pub trait RaftFunctions: Send + Sync {

    /// ## Description
    /// Tick election (Follower or Candidate) 
    /// or tick heartbeat (Leader).
    /// ## Step
    /// This action may generate ready messages (should be handle):<br/>
    /// As ***(Pre)Candidate***:
    /// * MsgRequestPreVote (if enable_pre_vote_round)
    /// * MsgRequestVote<br/>
    /// *noting that: Follower will become (Pre)Candidate after call tick => tick_election*
    ///
    /// As ***Leader***:
    /// * MsgHeartbeat to all followers (candidates) in group
    fn tick(&mut self) -> bool;

    /// Generate a message with type `MsgHup`, and process it local.
    /// Then this peer will make an election and broadcast (pre)votes
    fn election(&mut self) -> Result<()>;

    /// ## Description
    /// Propose with binary operation log to cluster. the `operation_log` maybe 
    /// applied once quorum committed.
    /// ### Message
    /// MsgPropose when:
    /// * Leader: handle propose directly and may generate some appends (MsgAppend).
    /// * Follower: Forward this propose to leader.
    /// ### Error
    /// `ProposalDropped` with reason.
    fn propose(&mut self, operation_log: Vec<u8>) ->Result<()>;

    /// ## Description
    /// Same as the `propose`, the difference only the propose data,
    /// this method will generate `EntryConfChange` type entry
    fn propose_conf_change(&mut self, changed_conf: BatchConfChange) -> Result<()>;

    /// ## Description
    /// `CMD` is a special type propose content with `EntryType::Cmd`.
    /// It's often used to propose some commands to group, a command
    /// should be handled difference from `Normal` entry.
    fn propose_cmd(&mut self, cmd: Vec<u8>) -> Result<()>;

    /// ## Description
    /// Try broadcasting heartbeats to follower only if current peer is Leader.
    fn ping(&mut self);

    /// ## Description
    /// report specific node is unreachable, only leader can receive and handle
    /// this kind of request, then update this peer's progress to `Probe` state. 
    fn report_unreachable(&mut self, id: u64);

    fn tranfer_leadership(&mut self, transfee: u64) -> Result<()>;

    fn read_index(&mut self, request_context: Vec<u8>);

    /// Request a snapshot from a leader.
    /// The snapshot's index must be greater or equal to the request_index.
    fn request_snapshot(&mut self, request_index: u64) -> Result<()>;

    fn report_snapshot(&mut self, id: u64, status: SnapshotStatus);

    /// Apply proposal `BatchConfChange` at local.
    /// This action when handle by:
    /// * **Leader**: update joint members and conf_state. then maybe generate append 
    /// in type [MsgAppend](protos::raft_payload_proto::MessageType::MsgAppend) to follower who 
    /// is not in Replicate state. e.g. new incoming follower (voter).
    /// * **Follower**: only update joint members and 
    /// current [ConfState](protos::raft_log_proto::ConfState)
    fn apply_conf_change(&mut self, changed_conf: &BatchConfChange) -> Result<ConfState>;
}

impl<S: Storage> RaftFunctions for RaftNode<S> where S: Storage {

    fn tick(&mut self) -> bool {
        match self.raft.current_raft_role {
            Follower | Candidate | PreCandidate => self.raft.tick_election(),
            Leader => self.raft.tick_heartbeat()
        }
    }

    fn election(&mut self) -> Result<()> {
        let mut election_message = RaftMessage::default();
        election_message.set_msg_type(MessageType::MsgHup);
        self.raft.process(election_message)
    }

    #[inline]
    fn propose(&mut self, proposal_content: Vec<u8>) ->Result<()> {
        self.propose_with_ctx(vec![], proposal_content)
    }

    #[inline]
    fn propose_conf_change(&mut self, changed_conf: BatchConfChange) -> Result<()> {
        self.propose_conf_change_with_ctx(vec![], changed_conf)
    }

    #[inline]
    fn propose_cmd(&mut self, cmd: Vec<u8>) -> Result<()> {
        self.propose_cmd_with_ctx(vec![], cmd)
    }

    #[inline]
    fn ping(&mut self) {
        self.raft.ping();
    }

    fn tranfer_leadership(&mut self, transfee: u64) -> Result<()> {
        let mut transfer_leader = RaftMessage::default();
        transfer_leader.set_msg_type(MessageType::MsgTransferLeader);
        transfer_leader.from = transfee;
        self.raft.process(transfer_leader)
    }

    fn report_unreachable(&mut self, id: u64) {
        let mut unreachable = RaftMessage::default();
        unreachable.set_msg_type(MessageType::MsgUnreachable);
        unreachable.from = id;
        let _ = self.raft.process(unreachable);
    }

    fn read_index(&mut self, request_context: Vec<u8>) {
        let mut read_index = RaftMessage::default();
        read_index.set_msg_type(MessageType::MsgReadIndex);
        let mut entry = Entry::default();
        entry.data = request_context;
        read_index.entries = vec![entry];
        let _ = self.raft.process(read_index);
    }

    #[inline]
    fn request_snapshot(&mut self, request_index: u64) -> Result<()> {
        self.raft.request_snapshot(request_index)
    }

    fn report_snapshot(&mut self, id: u64, status: SnapshotStatus) {
        let reject = status == SnapshotStatus::Failure;
        let mut snap_status = RaftMessage::default();
        snap_status.set_msg_type(MessageType::MsgSnapStatus);
        snap_status.from = id;
        snap_status.reject = reject;
        let _ = self.raft.process(snap_status);
    }

    #[inline]
    fn apply_conf_change(&mut self, changed_conf: &BatchConfChange) -> Result<ConfState> {
        self.raft.apply_conf_changes(&changed_conf)
    }
}


impl<S> RaftNode<S> where S: Storage {
    
    /// propose a byte array proposal to all followers in the cluster
    pub fn propose_with_ctx(&mut self, context: Vec<u8>, proposal_content: Vec<u8>) -> Result<()> {
        let mut proposal = RaftMessage::default();
        proposal.set_msg_type(MessageType::MsgPropose);
        proposal.from = self.raft.id;
        let mut entry = Entry::default();
        entry.data = proposal_content;
        entry.context = context;
        proposal.entries = vec![entry];
        self.raft.process(proposal)
    }

    pub fn propose_conf_change_with_ctx(&mut self, context: Vec<u8>, changed_conf: BatchConfChange) -> Result<()> {
        let mut proposal_conf = RaftMessage::default();
        proposal_conf.set_msg_type(MessageType::MsgPropose);
        
        let mut entry = Entry::default();
        entry.set_entry_type(EntryType::EntryConfChange);
        let mut buffer = BytesMut::new();
        changed_conf.encode(&mut buffer).unwrap();
        entry.data = buffer.to_vec();
        entry.context = context;
        proposal_conf.entries = vec![entry];
        self.raft.process(proposal_conf) 
    }

    /// propose a byte array proposal to all followers in the cluster
    pub fn propose_cmd_with_ctx(&mut self, context: Vec<u8>, cmd: Vec<u8>) -> Result<()> {
        let mut proposal = RaftMessage::default();
        proposal.set_msg_type(MessageType::MsgPropose);
        proposal.from = self.raft.id;
        let mut entry = Entry::default();
        entry.set_entry_type(EntryType::EntryCmd);
        entry.data = cmd;
        entry.context = context;
        proposal.entries = vec![entry];
        self.raft.process(proposal)
    }
}