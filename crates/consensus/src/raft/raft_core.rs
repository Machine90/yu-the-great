use crate::protos::{
    raft_log_proto::Entry,
    raft_payload_proto::{Message, MessageType::*},
};

use super::read_only::{ReadOnly, ReadOnlyOption, ReadState};
use super::{
    raft_log::RaftLog, raft_role::RaftRole, DEFAULT_INITIAL_TERM, DUMMY_ID, DUMMY_INDEX,
};
use crate::{config::Config, raft::DUMMY_TERM, storage::Storage};
use crate::{raft::UncommittedState, storage::NO_LIMIT};
use std::ops::{Deref, DerefMut};

pub struct RaftConf {
    pub id: u64,
    pub max_inflight_msgs: usize,
    pub max_msg_size: u64,
    /// Default to ReadOnlyOption::Safe, Setting this to `LeaseBased` requires `check_quorum = true`.
    pub read_only: ReadOnly,
    pub check_quorum: bool,
    pub enable_pre_vote: bool,
    pub skip_bcast_commit: bool,
    pub batch_append: bool,
    pub heartbeat_timeout: usize,
    pub election_timeout: usize,
    pub min_election_timeout: usize,
    pub max_election_timeout: usize,
    pub priority: u64,
    pub broadcast_became_leader: bool,
}

impl RaftConf {
    pub fn new(conf: Config) -> Self {
        RaftConf {
            id: conf.id,
            election_timeout: conf.tick_election_timeout,
            heartbeat_timeout: conf.tick_heartbeat_timeout,
            enable_pre_vote: conf.enable_pre_vote_round,
            check_quorum: conf.should_check_quorum,
            max_inflight_msgs: conf.max_inflight_messages,
            max_msg_size: conf.max_entries_size_per_message,
            skip_bcast_commit: !conf.broadcast_commit_enable,
            batch_append: false,
            min_election_timeout: conf.min_election_tick(),
            max_election_timeout: conf.max_election_tick(),
            priority: 0,
            read_only: ReadOnly::new(ReadOnlyOption::Safe, conf.enable_read_batch),
            broadcast_became_leader: conf.broadcast_became_leader,
        }
    }

    #[inline]
    pub fn is_skip_bcast_commit(&self) -> bool {
        self.skip_bcast_commit
    }
    #[inline]
    pub fn is_batch_append(&self) -> bool {
        self.batch_append
    }
    #[inline]
    pub fn min_election_timeout(&self) -> usize {
        self.min_election_timeout
    }
    #[inline]
    pub fn max_election_timeout(&self) -> usize {
        self.max_election_timeout
    }

    /// Update `read_only.option` to given option if enable `check_quorum` 
    /// or `force` to update.
    /// ### Returns 
    /// * **ReadOnlyOption**: Previous option. Maybe current option if has not updated.
    #[inline]
    pub fn update_read_only_option(
        &mut self, 
        updated: ReadOnlyOption, 
        force: bool
    ) -> ReadOnlyOption {
        let ori_opt = self.read_only.option;
        if !self.check_quorum {
            if force {
                self.check_quorum = true;
                self.read_only.option = updated;
            }
            return ori_opt;
        }
        self.read_only.option = updated;
        ori_opt
    }

    #[inline]
    pub fn read_only_option(&self) -> ReadOnlyOption {
        self.read_only.option
    }
}

/// The reference to Raft which we call RaftCore
impl<STORAGE: Storage> DerefMut for RaftCore<STORAGE> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conf
    }
}

impl<STORAGE: Storage> Deref for RaftCore<STORAGE> {
    type Target = RaftConf;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.conf
    }
}

pub struct RaftCore<S: Storage> {
    pub term: u64,
    pub vote: u64,
    pub read_states: Vec<ReadState>,
    pub raft_log: RaftLog<S>,
    pub pending_request_snapshot: u64,
    pub current_raft_role: RaftRole,
    promotable: bool,
    pub leader_id: u64,
    pub lead_transferee: Option<u64>,
    pub pending_conf_index: u64,
    pub election_elapsed: usize,
    pub heartbeat_elapsed: usize,
    pub randomized_election_timeout: usize,
    uncommitted_state: UncommittedState,
    conf: RaftConf,
}

impl<S: Storage> RaftCore<S> {
    pub fn new(storage: S, conf: Config) -> Self {
        Self::from_conf(storage, RaftConf::new(conf))
    }

    pub fn from_conf(storage: S, conf: RaftConf) -> Self {
        RaftCore {
            term: DEFAULT_INITIAL_TERM,
            vote: Default::default(),
            read_states: Default::default(),
            raft_log: RaftLog::new(storage),
            pending_request_snapshot: DUMMY_INDEX,
            current_raft_role: RaftRole::Follower,
            promotable: false,
            leader_id: Default::default(),
            lead_transferee: None,
            pending_conf_index: Default::default(),
            election_elapsed: Default::default(),
            heartbeat_elapsed: Default::default(),
            randomized_election_timeout: Default::default(),
            uncommitted_state: UncommittedState {
                max_uncommitted_size: NO_LIMIT as usize, // TODO use default unlimited now
                uncommitted_size: 0,
                last_log_tail_index: 0,
            },
            conf,
        }
    }

    /// Stash mail to given mailbox, it's abstract RPC send.
    /// This method do not send message really, instead it'll stash the
    /// request message to `raft.messages` and waitting for 'flushing'
    /// them to real network transport
    pub fn send_to_mailbox(&self, mut mail: Message, mailbox: &mut Vec<Message>) {
        if mail.from == DUMMY_ID {
            mail.from = self.id;
        }
        crate::trace!("Sending {}", mail);
        match mail.msg_type() {
            // All {pre-,}campaign messages need to have the term set when
            // sending.
            // - MsgVote: m.Term is the term the node is campaigning for,
            //   non-zero as we increment the term when campaigning.
            // - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
            //   granted, non-zero for the same reason MsgVote is
            // - MsgPreVote: m.Term is the term the node will campaign,
            //   non-zero as we use m.Term to indicate the next term we'll be
            //   campaigning for
            // - MsgPreVoteResp: m.Term is the term received in the original
            //   MsgPreVote if the pre-vote was granted, non-zero for the
            //   same reasons MsgPreVote is
            MsgRequestPreVote
            | MsgRequestVote
            | MsgRequestPreVoteResponse
            | MsgRequestVoteResponse => {
                if mail.term == DUMMY_TERM {
                    panic!("term should be set when sending {:?}", mail.msg_type());
                }
            }
            _ => {
                if mail.term != DUMMY_TERM {
                    panic!(
                        "term({}) should not be set when sending {:?}",
                        mail.term,
                        mail.msg_type()
                    );
                }
                // do not attach term to MsgPropose, MsgReadIndex
                // proposals are a way to forward to the leader and
                // should be treated as local message.
                // MsgReadIndex is also forwarded to leader.
                if mail.msg_type() != MsgPropose && mail.msg_type() != MsgReadIndex {
                    mail.term = self.term;
                }
            }
        }
        if mail.msg_type() == MsgRequestVote || mail.msg_type() == MsgRequestPreVote {
            mail.priority = self.priority;
        }
        mailbox.push(mail);
    }

    #[inline] pub fn promotable(&self) -> bool {
        self.promotable
    }

    #[inline] pub fn set_promotable(&mut self, promotable: bool) {
        self.promotable = promotable
    }

    #[inline]
    pub fn has_pending_conf(&self) -> bool {
        self.pending_conf_index > self.raft_log.get_applied()
    }

    #[inline]
    pub fn abort_leader_transfer(&mut self) {
        self.lead_transferee = None;
    }

    #[inline]
    pub fn pass_election_timeout(&self) -> bool {
        self.election_elapsed >= self.randomized_election_timeout
    }

    #[inline]
    pub fn uncommitted_state(&self) -> &UncommittedState {
        &self.uncommitted_state
    }

    #[inline]
    pub fn reset_uncommitted_tail_to(&mut self, last_index: u64) {
        self.uncommitted_state.uncommitted_size = 0;
        self.uncommitted_state.last_log_tail_index = last_index;
    }

    /// Try to increase the size of `reduce_entries` from `uncommitted_state`
    pub fn try_incr_uncommitted_size(&mut self, entries_incr: &[Entry]) -> bool {
        // self.uncommitted_size
        self.uncommitted_state
            .maybe_increase_uncommitted_size(entries_incr)
    }

    pub fn try_decr_uncommitted_size(&mut self, entries_reduce: &[Entry]) -> bool {
        self.uncommitted_state
            .maybe_reduce_uncommitted_size(entries_reduce)
    }

    /// Get the entries size of `uncommitted_state`
    #[inline]
    pub fn get_uncommitted_size(&self) -> usize {
        self.uncommitted_state.uncommitted_size
    }
}
