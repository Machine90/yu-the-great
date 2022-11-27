use raft_role::{raft_follower::FollowerRaft, raft_candidate::CandidateRaft};
use rand::Rng;
use crate::{debug, trace, error};
use std::{
    mem,
    ops::{Deref, DerefMut},
};

use self::{
    raft_cases::RaftCases,
    raft_core::RaftCore,
    raft_role::{raft_leader::LeaderRaft, RaftRole},
    raft_tracker::{ProgressTracker, RaftManager, TallyVoteResult},
};
use crate::{confchange::cluster_changer::*, config::Config, quorum::VoteResult, raft::read_only::{ReadOnly, ReadState}, storage::{NO_LIMIT, Storage}};

use crate::errors::*;
use crate::quorum::{Quorum, VoteResult::*};
use crate::protos::{
    raft_conf_proto::BatchConfChange,
    raft_log_proto::{ConfState, Entry, EntryType, HardState, Snapshot},
    raft_payload_proto::{self as payload, Message, MessageType},
};

use crate::protos::extends::raft_confchange_ext::ChangeEvent::*;

/// Declare sub-modules for raft module.
pub mod raft_cases;
pub mod raft_core;
pub mod raft_log;
pub mod raft_role;
pub mod raft_tracker;
pub mod read_only;

/// Default initial term is 0
pub const DEFAULT_INITIAL_TERM: u64 = 0;
/// Default to 0, the DUMMY_TERM also is the DEFAULT_INITIAL_TERM <br/>
/// term of quorum should always greater than DEFAULT_INITIAL_TERM after first election
pub const DUMMY_TERM: u64 = DEFAULT_INITIAL_TERM;

pub const DUMMY_ID: u64 = 0;
/// Default to 0, use to determine if index is a invalid value
pub const DUMMY_INDEX: u64 = 0;

pub const CAMPAIGN_TRANSFER: &[u8] = b"CampaignTransfer";
pub const CAMPAIGN_PRE_ELECTION: &[u8] = b"CampaignPreElection";
pub const CAMPAIGN_ELECTION: &[u8] = b"CampaignElection";

pub struct Raft<STORAGE: Storage> {
    pub core: raft_core::RaftCore<STORAGE>,
    pub tracker: raft_tracker::ProgressTracker,
    pub messages: Vec<payload::Message>,
}

/// The reference to Raft which we call RaftCore
impl<STORAGE: Storage> DerefMut for Raft<STORAGE> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.core
    }
}

impl<STORAGE: Storage> Deref for Raft<STORAGE> {
    type Target = raft_core::RaftCore<STORAGE>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl<STORAGE: Storage> Raft<STORAGE> {
    pub fn new(storage: STORAGE, conf: Config) -> Result<Raft<STORAGE>> {
        let raft_initial_state = storage.initial_state()?;
        let config_changes = &raft_initial_state.conf_state;
        let voters = &config_changes.voters;
        let learners = &config_changes.learners;

        let tracker = raft_tracker::ProgressTracker::initial_capacity(
            voters.len(),
            learners.len(),
            conf.max_inflight_messages,
        );
        let core = RaftCore::new(storage, conf);
        let mut raft = Raft {
            core,
            tracker,
            messages: Default::default(),
        };

        ClusterChanger::restore(
            &mut raft.tracker,
            raft.core.raft_log.last_index().unwrap(),
            config_changes,
        )?;
        let new_config_changes = raft.post_cluster_conf_change();
        if !ConfState::is_conf_state_eq(&new_config_changes, config_changes) {
            panic!(
                "invalid restore initial conf: {:?} is not eq to {:?}",
                config_changes,
                new_config_changes
            );
        }

        if raft_initial_state.hard_state != HardState::default() {
            raft.init_with_state(&raft_initial_state.hard_state);
        }

        // If applied index has been set, then update
        // peer's applied to config value
        if conf.applied > 0 {
            raft.commit_apply(conf.applied);
        }

        // then current peer act as a follower without leader
        raft.become_follower(raft.core.term, DUMMY_ID);
        debug!(
            "Raft peer create success, see:";
            "term" => raft.term,
            "quorum committed index" => raft.raft_log.quorum_committed,
            "applied index" => raft.raft_log.get_applied(),
            "last index" => raft.raft_log.last_index().unwrap(),
            "last term" => raft.raft_log.last_term(),
            "cluster peers" => ?raft.tracker.quorum
        );
        Ok(raft)
    }

    pub fn init_with_state(&mut self, state: &HardState) {
        let (allowed_min_index, allowed_max_index) = (
            self.raft_log.quorum_committed,
            self.raft_log.last_index().unwrap(),
        );
        if state.commit < allowed_min_index || state.commit > allowed_max_index {
            panic!(
                "initial hard state commit: {} is out of range: [{}..{}]",
                state.commit,
                allowed_min_index,
                allowed_max_index
            )
        }
        self.raft_log.quorum_committed = state.commit;
        self.term = state.term;
        self.vote = state.vote;
    }

    pub fn store(&self) -> &STORAGE {
        &self.raft_log.store
    }

    pub fn mut_store(&mut self) -> &mut STORAGE {
        &mut self.raft_log.store
    }

    pub fn tracker(&self) -> &ProgressTracker {
        &self.tracker
    }

    pub fn mut_tracker(&mut self) -> &mut ProgressTracker {
        &mut self.tracker
    }

    pub fn snapshot_ref(&self) -> Option<&Snapshot> {
        self.raft_log.unstable.snapshot.as_ref()
    }

    pub fn soft_state(&self) -> SoftState {
        SoftState {
            leader_id: self.leader_id,
            raft_state: self.current_raft_role,
        }
    }

    pub fn hard_state(&self) -> HardState {
        let mut hs = HardState::default();
        hs.commit = self.raft_log.quorum_committed;
        hs.term = self.term;
        hs.vote = self.vote;
        hs
    }

    pub fn set_rand_election_timeout(&mut self, timeout: usize) {
        assert!(self.min_election_timeout <= timeout && timeout < self.max_election_timeout);
        self.randomized_election_timeout = timeout
    }

    pub fn enable_group_commit(&mut self, enable: bool) {
        self.mut_tracker().enable_group_commit(enable);

        let disable_group_commit = !enable;
        if self.current_raft_role == RaftRole::Leader && disable_group_commit {
            (self as &mut dyn LeaderRaft).try_commit_and_broadcast();
        }
    }

    #[doc(hidden)]
    pub fn apply_conf_changes(&mut self, batch_change: &BatchConfChange) -> Result<ConfState> {
        let mut changer = ClusterChanger::new(&self.tracker);
        // judge this batch is joint operation or simple change, then 
        // handle them in the right way.
        let (cluster, changes) = match batch_change.get_change_event() {
            (EnterJoint, Some(auto_leave)) => {
                changer.enter_joint(&batch_change.changes, auto_leave)?
            }
            (LeaveJoint, None) => changer.leave_joint()?,
            (Simple, None) => changer.simple(&batch_change.changes)?,
            _ => {
                panic!("not support such change");
            }
        };
        // then apply these changes to raft's state machine
        self.tracker
            .apply_cluster_changes(cluster, changes, self.raft_log.last_index().unwrap());
        // finally, broadcast or append changes (maybe some commit) to others if Leader apply the changes.
        Ok(self.post_cluster_conf_change())
    }

    fn post_cluster_conf_change(&mut self) -> ConfState {
        let conf_state = self.tracker.to_conf_state();
        let is_voter = self.tracker.quorum.contain_voter(self.id);
        self.set_promotable(is_voter);
        if !is_voter && self.current_raft_role == RaftRole::Leader {
            return conf_state;
        }

        if self.current_raft_role != RaftRole::Leader || conf_state.voters.is_empty() {
            return conf_state;
        }

        // only the current peer is Leader should broadcast configure changes of the cluster
        (self as &mut dyn LeaderRaft).broadcast_cluster_conf_change();
        conf_state
    }

    #[inline]
    pub fn commit_apply(&mut self, update_applied: u64) {
        let origin_applied = self.raft_log.get_applied();
        self.raft_log.update_applied_index(update_applied);

        let not_pending_conf_change =
            self.pending_conf_index >= origin_applied && self.pending_conf_index <= update_applied;

        if self.current_raft_role == RaftRole::Leader
            && self.tracker.auto_leave
            && not_pending_conf_change
        {
            let mut empty_conf = Entry::default();
            // this empty confchange entry is used to generate 
            // a LeaveJoint event to clear outgoing voters.
            empty_conf.set_entry_type(EntryType::EntryConfChange);
            if !self.append_entries(&mut vec![empty_conf]) {
                panic!("appending an empty EntryConfChange should never be dropped")
            }
            self.pending_conf_index = self.raft_log.last_index().unwrap();
            debug!("initiating automatic transition out of joint cluster"; "cluster info" => ?self.tracker.cluster_info());
        }
    }

    /// Since current raft received message from other rafts in the cluster,
    /// this method will be call. And we handle all kinds of cases in this
    /// method.
    ///
    /// # Raft Cases
    pub fn process(&mut self, message: payload::Message) -> Result<()> {
        // when received message from other rafts in case...
        let msg_type = message.msg_type();
        let me = self.id;
        let case = RaftCases::receive(self, &message);
        trace!(
            "[RaftCases] {} receive msg: {:?} and step in case: {c}",
            me,
            msg_type,
            c = &case
        );
        match case {
            RaftCases::RequestElection => self.hup(false),
            RaftCases::SplitBrain => {
                // then tell the leader that you're not leader anymore, and now term is...
                let response = short_message(None, message.from, MessageType::MsgAppendResponse);
                self.core.send_to_mailbox(response, &mut self.messages);
            }
            RaftCases::LowerTermCandidatePreVote => {
                // then tell him that you're rejected and now term is...
                let mut reject =
                    reject_message(None, message.from, MessageType::MsgRequestPreVoteResponse);
                reject.term = self.term;
                self.core.send_to_mailbox(reject, &mut self.messages);
            }
            RaftCases::RecvLowerTerm => {
                debug!(
                    "Ignore when {case} from {from}", case = &case, from = message.from;
                    "term" => self.term, "msg type" => ?message.msg_type(), "msg term" => message.term
                );
            }
            RaftCases::LeaderRecvMsg => {
                return (self as &mut dyn LeaderRaft).process(message);
            }
            RaftCases::FollowerRecvMsg => {
                return (self as &mut dyn FollowerRaft).process(message);
            }
            RaftCases::CandidateRecvMsg => {
                return (self as &mut dyn CandidateRaft).process(message);
            }
            // when received a normal msg (received term equals to current raft term)
            RaftCases::ApproveRequestPreVote => {
                self.approve_vote(&message);
            }
            RaftCases::ApproveRequestVote => {
                self.approve_vote(&message);
                // reset the election elapsed and record message sender as vote for
                self.election_elapsed = 0;
                self.vote = message.from;
            }
            RaftCases::RejectRequestVote => {
                // reject the (pre)vote request
                self.log_vote_info(&message, false);
                let (response_to, vote_type) = (message.from, message.msg_type());
                let mut reject_response =
                    reject_message(None, response_to, Self::map_request_to_response(vote_type));
                reject_response.term = self.term;
                let (my_index, my_term) = self.raft_log.commit_info();
                reject_response.commit = my_index;
                reject_response.commit_term = my_term;
                self.core.send_to_mailbox(reject_response, &mut self.messages);
                let _ = self.maybe_commit_by_vote(&message);
            }
            RaftCases::HighTermPeerVoteWhenInLease => {
                debug!(
                    "local: [term: {term}, index: {index}, vote: {vote}] recv msg from \
                    {remote}: [term: {rterm}, index: {rindex}] was ignored when {case}",
                    term = self.raft_log.last_term(),
                    index = self.raft_log.last_index().unwrap(),
                    vote = self.vote,
                    remote = message.from,
                    rterm = message.term,
                    rindex = message.index,
                    case = &case,
                );
            }
        }
        Ok(())
    }

    pub fn map_request_to_response(msg_type: MessageType) -> MessageType {
        match msg_type {
            MessageType::MsgRequestVote => MessageType::MsgRequestVoteResponse,
            MessageType::MsgRequestPreVote => MessageType::MsgRequestPreVoteResponse,
            _ => panic!("Not a vote message: {:?}", msg_type),
        }
    }

    #[inline]
    fn approve_vote(&mut self, message: &Message) {
        self.log_vote_info(message, true);
        let mut approve_response = accept_message(
            None,
            message.from,
            Self::map_request_to_response(message.msg_type()),
        );
        approve_response.term = message.term;
        self.core.send_to_mailbox(approve_response, &mut self.messages);
    }

    /// Try starting an election by: 
    /// * transfer leadership (if transfer_leader is true)
    /// * request pre election
    /// * request election
    fn hup(&mut self, transfer_leader: bool) {
        if self.current_raft_role == RaftRole::Leader {
            debug!("ignoring MsgHup because already leader");
            return;
        }
        match self.raft_log.log_state(None) {
            raft_log::LogState::ConfChangePending => {
                crate::warn!(
                    "cannot campaign at term {term} since there are still pending \
                    group changes to apply",
                    term = self.term
                );
                return;
            }
            _ => (),
        }

        trace!("{me} starting a new election", me = self.id; "term" => self.term);
        if transfer_leader {
            self.campaign(CAMPAIGN_TRANSFER); // transfer leader
        } else if self.enable_pre_vote {
            self.campaign(CAMPAIGN_PRE_ELECTION); // follower => pre-candidate
        } else {
            self.campaign(CAMPAIGN_ELECTION); // follower => candidate
        }
    }

    fn campaign(&mut self, campaign_type: &[u8]) {
        let (vote_type, term) = if campaign_type == CAMPAIGN_PRE_ELECTION {
            self.become_pre_candidate();
            (MessageType::MsgRequestPreVote, self.term + 1)
        } else {
            self.become_candidate();
            (MessageType::MsgRequestVote, self.term)
        };

        let my_id = self.id;
        // record my vote (always true, vote for myself)
        // only one voter(me) then won the election immediately
        if Won == self.poll_and_handle_votes(my_id, vote_type, true) {
            return;
        }

        let (commit, commit_term) = self.raft_log.commit_info();
        // create broadcast vote message to voters.
        for to_peer_id in self.tracker.all_voters() {
            if my_id == to_peer_id {
                continue;
            }
            let mut message = short_message(None, to_peer_id, vote_type);
            message.term = term;
            message.index = self.raft_log.last_index().unwrap();
            message.log_term = self.raft_log.last_term();
            message.commit = commit;
            message.commit_term = commit_term;
            if campaign_type == CAMPAIGN_TRANSFER {
                message.context = campaign_type.to_vec();
            }
            self.core.send_to_mailbox(message, &mut self.messages);
        }
    }

    /// ## Description
    /// Judge vote result once a vote_response reached. <br/>
    /// Become `Leader` immediately if grant votes has reached majority <br/>
    /// or become `Follower` immediately if reject votes has reached majority <br/>
    /// or become `Candidate` if current peer is `Precandidate`
    /// without waitting for all peer's acked. <br/>
    /// Other wise keep pending and wait for more peer's vote acked until reached
    /// majority.
    fn poll_and_handle_votes(&mut self, from: u64, mt: MessageType, vote: bool) -> VoteResult {
        let (grants, rejects, vote_result) = self.poll(from, vote);
        if from != self.id {
            debug!(
                "received vote ack from {from}", from = from;
                "vote me?" => vote,
                "msg type" => ?mt,
                "term" => self.term,
                "approvals" => grants,
                "rejections" => rejects,
            );
        }
        match vote_result {
            Won => {
                if self.current_raft_role == RaftRole::PreCandidate {
                    // when pre-candidate recv response, then transfer role to candidate.
                    self.campaign(CAMPAIGN_ELECTION); // pre-candidate => candidate
                } else {
                    self.become_leader();
                    if self.broadcast_became_leader {
                        self.broadcast_append();
                    }
                }
            }
            Lost => {
                self.become_follower(self.term, DUMMY_ID);
            }
            Pending => (),
        }
        vote_result
    }

    fn poll(&mut self, from: u64, vote: bool) -> TallyVoteResult {
        self.tracker.record_vote(from, vote);
        self.tracker.tally_votes()
    }

    #[must_use]
    fn append_entries(&mut self, entries: &mut Vec<Entry>) -> bool {
        if !self.try_incr_uncommitted_size(entries) {
            return false;
        }

        let last_idx = self.raft_log.last_index().unwrap();
        for (i, entry) in entries.iter_mut().enumerate() {
            entry.term = self.term;
            entry.index = last_idx + 1 + i as u64;
        }
        self.raft_log.append(entries.as_slice());
        true
    }

    /// There has only one way to became **Follower** with legal `leader_id` (not INVALID_ID 0): <br/>
    /// Receive a Message with type: **MsgAppend | MsgHeartbeat | MsgSnapshot** <br/>
    /// from a legal **Leader**
    pub fn become_follower(&mut self, term: u64, leader_id: u64) {
        let pending_request_snapshot = self.pending_request_snapshot;
        self.reset(term);

        self.leader_id = leader_id;
        self.current_raft_role = RaftRole::Follower;
        self.pending_request_snapshot = pending_request_snapshot;
        trace!(
            "{me} became follower of {leader} at term {term}",
            me = self.id,
            leader = leader_id,
            term = self.term
        );
    }

    pub fn become_pre_candidate(&mut self) {
        assert_ne!(
            self.current_raft_role,
            RaftRole::Leader,
            "invalid role transfer: [Leader ==> PreCandidate]"
        );
        self.current_raft_role = RaftRole::PreCandidate;
        self.tracker.reset_votes();
        self.leader_id = DUMMY_ID;
        trace!(
            "{me} become pre-candidate at term: {term}",
            me = self.id,
            term = self.term
        );
    }

    pub fn become_candidate(&mut self) {
        assert_ne!(
            self.current_raft_role,
            RaftRole::Leader,
            "invalid role transfer: [Leader ==> Candidate]"
        );
        let term = self.term + 1;
        self.reset(term);
        let me = self.id;
        self.vote = me;
        self.current_raft_role = RaftRole::Candidate;
        trace!(
            "{me} become candidate at term: {term}",
            me = me,
            term = term
        );
    }

    pub fn become_leader(&mut self) {
        trace!("prepare to become leader");
        assert_ne!(
            RaftRole::Follower,
            self.current_raft_role,
            "invalid role transfer: [Follower ==> Leader]"
        );
        let ori_term = self.term;
        self.reset(ori_term);
        self.leader_id = self.id;
        self.current_raft_role = RaftRole::Leader;

        let last_index = self.raft_log.last_index().unwrap();
        assert_eq!(last_index, self.raft_log.get_persisted());

        self.reset_uncommitted_tail_to(last_index);

        let me = self.id;
        let my_progress = self.mut_tracker().get_mut(me).unwrap();
        my_progress.enter_replicate();
        self.pending_conf_index = last_index;

        if !self.append_entries(&mut vec![Entry::default()]) {
            panic!("should not drop the empty entry");
        }

        debug!(
            "{me} become leader at term: {term}",
            me = me,
            term = self.term
        );
    }

    fn maybe_commit_by_vote(&mut self, vote: &Message) -> bool {
        if vote.commit == DUMMY_INDEX || vote.commit_term == DUMMY_TERM {
            return false;
        }
        let last_committed = self.raft_log.quorum_committed;
        if vote.commit <= last_committed || self.current_raft_role == RaftRole::Leader {
            return false;
        }

        // try to commit campaigner's index, term to current peer's raft_log
        // this action may affect the (Pre)Candidate, Follower
        if !self
            .raft_log
            .maybe_commit(vote.commit, vote.commit_term)
        {
            return false;
        }

        let raft_log = &mut self.core.raft_log;
        debug!(
            "[commit: {:?}, last-index: {:?}, last-term: {:?}] fast-forwarded commit to vote [index: {:?}, term: {:?}]",
            raft_log.quorum_committed, 
            raft_log.last_index(), 
            raft_log.last_term(), 
            vote.commit, 
            vote.commit_term
        );

        if self.current_raft_role != RaftRole::Candidate
            && self.current_raft_role != RaftRole::PreCandidate
        {
            return true;
        }
        // detect if there exists some confchange in unstable.
        match self.raft_log.log_state(Some(last_committed + 1)) {
            // The candidate doesn't have to step down in theory, here just for best
            // safety as we assume quorum won't change during election.
            raft_log::LogState::ConfChangePending => {
                debug!("became follower after commit by vote {vote}");
                self.become_follower(self.term, DUMMY_ID)
            }
            _ => (),
        }
        true
    }

    #[inline]
    pub fn uncommitted_size(&self) -> usize {
        self.uncommitted_state().uncommitted_size
    }

    fn log_vote_info(&self, msg: &Message, approve: bool) {
        let hint = if approve {
            "cast vote for "
        } else {
            "reject vote from"
        };
        let (lindex, lterm) = (
            self.raft_log.last_index().unwrap(),
            self.raft_log.last_term(),
        );
        trace!(
            "local: [term: {lterm} index: {lindex}, vote: {vote}] {hint} peer: {campaigner}: [term: {cterm} index: {cindex}] at term {term}",
            lterm = lterm, lindex = lindex, vote = self.vote,
            hint = hint, campaigner = msg.from,
            cterm = msg.term, cindex = msg.index, term = self.term
        );
    }

    fn reset(&mut self, given_term: u64) {
        if self.term != given_term {
            self.term = given_term;
            self.vote = DUMMY_ID;
        }
        self.leader_id = DUMMY_ID;
        self.reset_randomized_election_timeout();
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;

        self.abort_leader_transfer();

        self.tracker.reset_votes();

        self.pending_conf_index = 0;
        self.read_only = ReadOnly::new(self.read_only.option, self.read_only.enable_batch);
        self.pending_request_snapshot = DUMMY_INDEX;

        let last_index = self.raft_log.last_index().unwrap();
        let quorum_committed = self.raft_log.quorum_committed;
        let persisted = self.raft_log.get_persisted();
        let self_id = self.id;
        for (&id, mut progress) in self.mut_tracker().iter_mut() {
            progress.reset(last_index + 1);
            if id == self_id {
                progress.match_index = persisted;
                progress.committed_index = quorum_committed;
            }
        }
    }

    fn reset_randomized_election_timeout(&mut self) {
        let prev_timeout = self.randomized_election_timeout;
        let cur_timeout =
            rand::thread_rng().gen_range(self.min_election_timeout..=self.max_election_timeout);
        debug!(
            "reset election timeout {prev_timeout} -> {cur_timeout}",
            prev_timeout = prev_timeout,
            cur_timeout = cur_timeout
        );
        self.randomized_election_timeout = cur_timeout;
    }

    /// Receive read index message as leader and decide to...
    /// * If this msg from "me" or from "local": generate `ReadState` to read_states, 
    /// this read_state will be set to `Ready`.
    /// * If this msg from "other follower": generate a `MsgReadIndexResp` to mailbox directly.
    fn handle_ready_read_index(&mut self, mut req_msg: Message, index: u64) -> Option<Message> {
        if req_msg.from == DUMMY_ID || req_msg.from == self.id {
            // if the request from local, leader just take it out and set to ready.
            // this usually means client request read_index to leader directly without any followers forwarded.
            let read_state = ReadState {
                index,
                request_ctx: mem::take(&mut req_msg.entries[0].data),
            };
            // read states, Leader take this state to do really read logic. 
            self.read_states.push(read_state);
            // then nothing to response to (not forwarded from any follower)
            return None;
        }
        // otherwise received forwarded read_index request from other followers.
        // then just return.
        let mut msg_readindex_resp =
            short_message(None, req_msg.from, MessageType::MsgReadIndexResp);
        msg_readindex_resp.index = index;
        msg_readindex_resp.entries = req_msg.entries;
        Some(msg_readindex_resp)
    }

    pub fn persist_snapshot(&mut self, snapshot_index: u64) {
        self.raft_log.maybe_persist_snapshot(snapshot_index);
    }

    pub fn persist_entries(&mut self, index: u64, term: u64) {
        let is_update = self.raft_log.maybe_persist(index, term);
        if is_update && self.current_raft_role == RaftRole::Leader {
            if term != self.term {
                error!(
                    "leader's persisted index changed but the term {} is not the same as {}",
                    term, self.term
                );
            }
            let self_id = self.id;
            let progress = self.mut_tracker().get_mut(self_id).unwrap();
            if progress.try_update(index) {
                self.try_commit(self.should_broadcast_commit());
            }
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub struct SoftState {
    /// The potential leader of the cluster.
    pub leader_id: u64,
    /// The soft role this node may take.
    pub raft_state: RaftRole,
}

/// UncommittedState is used to keep track of information of uncommitted
/// log entries on 'leader' node
pub struct UncommittedState {
    /// Specify maximum of uncommitted entry size.
    /// When this limit is reached, all proposals to append new log will be dropped
    max_uncommitted_size: usize,

    /// Record current uncommitted entries size.
    uncommitted_size: usize,

    /// Record index of last log entry when node becomes leader from candidate.
    /// See https://github.com/tikv/raft-rs/pull/398#discussion_r502417531 for more detail
    last_log_tail_index: u64,
}

impl UncommittedState {
    #[inline]
    pub fn is_no_limit(&self) -> bool {
        self.max_uncommitted_size == NO_LIMIT as usize
    }

    pub fn maybe_increase_uncommitted_size(&mut self, ents: &[Entry]) -> bool {
        // fast path
        if self.is_no_limit() {
            return true;
        }

        let size: usize = ents.iter().map(|ent| (&ent.data).len()).sum();

        // 1. we should never drop an entry without any data(eg. leader election)
        // 2. we should allow at least one uncommitted entry
        // 3. add these entries will not cause size overlimit
        if size == 0
            || self.uncommitted_size == 0
            || size + self.uncommitted_size <= self.max_uncommitted_size
        {
            self.uncommitted_size += size;
            true
        } else {
            false
        }
    }

    pub fn maybe_reduce_uncommitted_size(&mut self, ents: &[Entry]) -> bool {
        // fast path
        if self.is_no_limit() || ents.is_empty() {
            return true;
        }

        // user may advance a 'Ready' which is generated before this node becomes leader
        let size: usize = ents
            .iter()
            .skip_while(|ent| ent.index <= self.last_log_tail_index)
            .map(|ent| (&ent.data).len())
            .sum();

        if size > self.uncommitted_size {
            self.uncommitted_size = 0;
            false
        } else {
            self.uncommitted_size -= size;
            true
        }
    }
}

/// all messages short-hand methods
///
fn reject_message(from: Option<u64>, to: u64, message_type: MessageType) -> Message {
    new_message(from, to, message_type, Some(true))
}

fn accept_message(from: Option<u64>, to: u64, message_type: MessageType) -> Message {
    new_message(from, to, message_type, Some(false))
}

fn short_message(from: Option<u64>, to: u64, message_type: MessageType) -> Message {
    new_message(from, to, message_type, None)
}

fn new_message(
    from: Option<u64>,
    to: u64,
    message_type: MessageType,
    reject: Option<bool>,
) -> Message {
    let mut m = Message::default();
    m.to = to;
    if let Some(id) = from {
        m.from = id;
    }

    if let Some(reject) = reject {
        m.reject = reject;
    }

    m.set_msg_type(message_type);
    m
}

/// Method for determine if `extend_entries` is continuous for `msg.entries`
pub fn is_continuous_entries(msg: &Message, extend_entries: &[Entry]) -> bool {
    if msg.entries.is_empty() || extend_entries.is_empty() {
        return true;
    }
    let expected_next_idx = msg.entries.last().unwrap().index + 1;
    return extend_entries.first().unwrap().index == expected_next_idx;
}
