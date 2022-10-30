use std::{cmp::min, mem};

use crate::errors::*;
use crate::protos::{
    raft_log_proto::{ConfState, Snapshot},
    raft_payload_proto::{Message as RaftMessage, MessageType::*},
};
use crate::{
    confchange::cluster_changer,
    raft::{
        accept_message, raft_tracker::RaftManager, read_only::ReadState, reject_message,
        short_message, Raft, DUMMY_ID, DUMMY_INDEX, DUMMY_TERM,
    },
    storage::Storage,
};
use crate::{warn, debug, error};
use super::RaftRole;

/// Traits for raft, we define these 3 kinds traits:
/// 1 Leader
/// 2 Follower
/// 3 Normal raft behavior
pub trait FollowerRaft {
    fn tick_election(&mut self) -> bool;

    fn process(&mut self, message: RaftMessage) -> Result<()>;

    fn request_snapshot(&mut self, request_index: u64) -> Result<()>;
}

impl<STORAGE: Storage> FollowerRaft for Raft<STORAGE> {
    fn tick_election(&mut self) -> bool {
        self.election_elapsed += 1;
        if !self.pass_election_timeout() || !self.promotable() {
            return false;
        }

        self.election_elapsed = 0;
        let msg_hup = short_message(Some(self.id), DUMMY_ID, MsgHup);
        let _ = self.process(msg_hup);
        true
    }

    fn process(&mut self, mut message: RaftMessage) -> Result<()> {
        let msg_from = message.from;
        match message.msg_type() {
            MsgPropose => {
                if self.leader_id == DUMMY_ID {
                    let reason = format!("there has not leader at term {:?}", self.term);
                    warn!("[follower-{:?}] {:?}, drop the propose.", self.id, reason);
                    return Err(Error::ProposalDropped(reason));
                }
                // if leader exists in cluster, then redirect proposal to it.
                message.to = self.leader_id;
                self.core.send_to_mailbox(message, &mut self.messages);
            }
            MsgAppend => {
                self.election_elapsed = 0;
                self.leader_id = msg_from;
                self.handle_append_entries(&message);
            }
            MsgHeartbeat => {
                self.election_elapsed = 0;
                self.leader_id = msg_from;
                self.handle_heartbeat(message);
            }
            MsgSnapshot => {
                self.election_elapsed = 0;
                self.leader_id = msg_from;
                self.handle_snapshot(message);
            }
            MsgTransferLeader => {
                if self.leader_id == DUMMY_ID {
                    debug!(
                        "no leader at term {term}; dropping leader transfer msg",
                        term = self.term
                    );
                    return Ok(());
                }
                message.to = self.leader_id;
                self.core.send_to_mailbox(message, &mut self.messages);
            }
            MsgReadIndex => {
                if self.leader_id == DUMMY_ID {
                    debug!(
                        "no leader at term {term}; dropping index reading msg",
                        term = self.term
                    );
                    return Ok(());
                }
                message.to = self.leader_id;
                self.core.send_to_mailbox(message, &mut self.messages);
            }
            MsgReadIndexResp => {
                let valid_entries_count: usize = 1;

                let (msg_index, msg_term) = (message.index, message.term);
                let entries_in_msg = message.entries.len();
                if entries_in_msg != valid_entries_count {
                    error!(
                        "invalid format of MsgReadIndexResp from {}",
                        msg_from;
                        "entries count" => entries_in_msg,
                    );
                    return Ok(());
                }
                let read_state = ReadState {
                    index: msg_index,
                    request_ctx: mem::take(&mut message.entries[0].data),
                };
                self.read_states.push(read_state);
                self.raft_log.maybe_commit(msg_index, msg_term);
            }
            MsgTimeoutNow => {
                if self.promotable() {
                    debug!(
                        "[term {term}] received MsgTimeoutNow from {from} and starts an election to \
                         get leadership.",
                        term = self.term,
                        from = msg_from
                    );
                    self.hup(true);
                } else {
                    debug!(
                        "received MsgTimeoutNow from {} but is not promotable",
                        msg_from
                    );
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn request_snapshot(&mut self, request_index: u64) -> Result<()> {
        if self.current_raft_role == RaftRole::Leader {
            debug!("can not request snapshot on leader; dropping request snapshot");
        } else if self.leader_id == DUMMY_ID {
            debug!("drop request snapshot because of no leader"; "term" => self.term);
        } else if self.snapshot_ref().is_some() || self.pending_request_snapshot != DUMMY_INDEX {
            debug!("there is a pending snapshot; dropping request snapshot");
        } else {
            self.pending_request_snapshot = request_index;
            self.send_request_snapshot();
            return Ok(());
        }
        Err(Error::RequestSnapshotDropped)
    }
}

trait FollowerPeer {
    /// ### Recv Msgs:
    /// Only two kinds of messages can step in this method:
    /// * Response for `Leader`'s request of making snapshot (`MsgSnapshot`).
    /// * Response for `Leader`'s `MsgAppend` or `MsgHeartBeat` if this peer still in make snapshot.
    /// ### Response Msgs
    /// * `MsgAppendResponse` with in reject
    fn send_request_snapshot(&mut self);
}

impl<S> FollowerPeer for Raft<S>
where
    S: Storage,
{
    fn send_request_snapshot(&mut self) {
        let mut req_snapshot = reject_message(None, self.leader_id, MsgAppendResponse);
        req_snapshot.index = self.raft_log.quorum_committed;
        req_snapshot.reject_hint = self.raft_log.last_index().unwrap();
        req_snapshot.request_snapshot = self.pending_request_snapshot;
        self.core.send_to_mailbox(req_snapshot, &mut self.messages);
    }
}

pub trait FollowerHandler {
    /// maybe return
    /// ## Message
    /// * MsgAppendResponse in aprove, but don't really update my state, because
    /// * MsgAppendResponse in reject, means I'm applying snapshot
    /// incomming index is lower than `quorum_committed` that known to me.
    /// * MsgAppendResponse in aprove if I have try append to `raft_log` success.
    /// * MsgAppendResponse in reject if I have failed to append to `raft_log`,
    /// of cause, I will tell you some failure hints what I know
    fn handle_append_entries(&mut self, message: &RaftMessage);
    fn handle_heartbeat(&mut self, message: RaftMessage);

    /// Receive a snapshot from leader and retore to local, then generate a
    /// [AppendResponse](crate::protos::raft_payload_proto::MessageType::MsgAppendResponse)
    fn handle_snapshot(&mut self, message: RaftMessage);

    /// Try restore from given snapshot, failure if
    /// * snapshot's index less than local committed.
    /// * current node is not follower (then become follower)
    /// * current node not in snapshot's voters (includes learners)
    /// * fast-forward to snapshot's index
    ///
    /// If success, then:
    /// * Restore snapshot to unstable and could be get in ready.
    /// * update self's progress in `ProgressTracker`
    fn restore_from_snapshot(&mut self, snapshot: Snapshot) -> bool;
}

impl<S> FollowerHandler for Raft<S>
where
    S: Storage,
{
    fn handle_append_entries(&mut self, message: &RaftMessage) {
        // check if still pending request snapshot, then response immediately
        if self.pending_request_snapshot != DUMMY_ID {
            self.send_request_snapshot();
            return;
        }
        let (quorum_committed, leader_id) = (self.raft_log.quorum_committed, message.from);

        // just response the latest index and committed of quorum to leader,
        // do not really append expired entries
        if message.index < quorum_committed {
            debug!(
                "got message with index({index}) lower than quorum index: {quorum}",
                index = message.index,
                quorum = quorum_committed
            );
            let mut response = short_message(None, leader_id, MsgAppendResponse);
            response.index = quorum_committed;
            response.commit = quorum_committed;
            self.core.send_to_mailbox(response, &mut self.messages);
            return;
        }

        let mut response;
        if let Some((_, last_appended_index)) = self.raft_log.maybe_append(
            message.index,
            message.log_term,
            message.commit,
            message.entries.as_slice(),
        ) {
            response = accept_message(None, leader_id, MsgAppendResponse);
            response.index = last_appended_index;
        } else {
            debug!(
                "reject MsgAppend [msg_log_term: {msg_log_term}, msg_index: {msg_index}] from {leader}",
                msg_log_term = message.log_term, msg_index = message.index, leader = leader_id;
                "peer's log_term" => ?self.raft_log.term(message.index)
            );

            // try to find some conflict hints for reject
            let hint_index = min(message.index, self.raft_log.last_index().unwrap());
            let (conflict_index, conflict_term) = self
                .raft_log
                .find_conflict_by_term(hint_index, message.term);

            // reject the leader's proposal if append raft_log failed
            response = reject_message(None, leader_id, MsgAppendResponse);
            response.index = message.index;
            response.reject_hint = conflict_index;
            response.log_term = conflict_term.unwrap_or(DUMMY_TERM);
        }

        response.commit = self.raft_log.quorum_committed;
        self.core.send_to_mailbox(response, &mut self.messages);
    }

    fn handle_heartbeat(&mut self, message: RaftMessage) {
        self.raft_log.commit_to(message.commit);
        if self.pending_request_snapshot != DUMMY_INDEX {
            self.send_request_snapshot();
            return;
        }

        let mut msg_heartbeat_resp = short_message(None, message.from, MsgHeartbeatResponse);
        msg_heartbeat_resp.context = message.context;
        msg_heartbeat_resp.commit = self.raft_log.quorum_committed;
        self.core
            .send_to_mailbox(msg_heartbeat_resp, &mut self.messages);
    }

    fn handle_snapshot(&mut self, mut message: RaftMessage) {
        let metadata = message.get_snapshot().get_metadata();
        let (snapshot_index, snapshot_term) = (metadata.index, metadata.term);
        let mut response = short_message(None, message.from, MsgAppendResponse);
        if self.restore_from_snapshot(message.take_snapshot()) {
            debug!(
                "[commit: {commit}, term: {term}] restored snapshot [index: {snapshot_index}, term: {snapshot_term}]",
                term = self.term,
                commit = self.raft_log.quorum_committed,
                snapshot_index = snapshot_index,
                snapshot_term = snapshot_term
            );
            response.index = self.raft_log.last_index().unwrap();
        } else {
            debug!(
                "[commit: {commit}, term: {term}] ignored snapshot [index: {snapshot_index}, term: {snapshot_term}]",
                term = self.term,
                commit = self.raft_log.quorum_committed,
                snapshot_index = snapshot_index,
                snapshot_term = snapshot_term
            );
            response.index = self.raft_log.quorum_committed;
        }
        self.core.send_to_mailbox(response, &mut self.messages);
    }

    fn restore_from_snapshot(&mut self, snapshot: Snapshot) -> bool {
        if snapshot.get_metadata().index < self.raft_log.quorum_committed {
            // ignore snapshot from leader, the index from snapshot will 
            // be `pending_request_snapshot`, and `pending_request_snapshot`
            // will be reset until leader `handle_snapshot_status`.
            return false;
        }

        if self.current_raft_role != RaftRole::Follower {
            warn!("non-follower attempted to restore snapshot"; "state" => ?self.current_raft_role);
            self.become_follower(self.term + 1, DUMMY_INDEX);
            return false;
        }

        let metadata = snapshot.get_metadata();
        let (snap_index, snap_term) = (metadata.index, metadata.term);
        let conf_state = metadata.get_conf_state();
        if conf_state
            .get_voters()
            .iter()
            .chain(conf_state.get_learners())
            .chain(conf_state.get_voters_outgoing())
            .all(|peer_id| *peer_id != self.id)
        {
            warn!("attempted to restore snapshot but it is not in the ConfState"; "conf_state" => ?conf_state);
            return false;
        }

        let log_committed = self.raft_log.quorum_committed;
        let log_last_index = self.raft_log.last_index().unwrap();
        let log_last_term = self.raft_log.last_term();

        if self.pending_request_snapshot == DUMMY_INDEX
            && self.raft_log.match_term(snap_index, snap_term)
        {
            debug!(
                "fast-forwarded commit to snapshot";
                "commit" => log_committed,
                "last_index" => log_last_index,
                "last_term" => log_last_term,
                "snapshot_index" => snap_index,
                "snapshot_term" => snap_term
            );
            self.raft_log.commit_to(snap_index);
            return false;
        }
        
        self.raft_log.restore(snapshot);

        let old_conf_state = self
            .core
            .raft_log
            .unstable_snapshot()
            .unwrap()
            .get_metadata()
            .get_conf_state();

        self.tracker.clear();

        let log_last_index = self.raft_log.last_index().unwrap();
        if let Err(err) = cluster_changer::ClusterChanger::restore(
            &mut self.tracker,
            log_last_index,
            old_conf_state,
        ) {
            panic!("unable to restore config {:?}: {}", old_conf_state, err);
        }

        let new_conf_state = self.post_cluster_conf_change();

        let old_conf_state = self
            .core
            .raft_log
            .unstable_snapshot()
            .unwrap()
            .get_metadata()
            .get_conf_state();

        if !ConfState::is_conf_state_eq(old_conf_state, &new_conf_state) {
            panic!(
                "invalid restore: {:?} != {:?}",
                old_conf_state, new_conf_state
            );
        }

        let me = self.id;
        let progress = self.tracker.get_mut(me).unwrap();
        progress.try_update(progress.next_index - 1);
        self.pending_request_snapshot = DUMMY_INDEX;

        debug!(
            "restore from snapshot success";
            "commit" => log_committed,
            "last_index" => log_last_index,
            "last_term" => log_last_term,
            "snapshot_index" => snap_index,
            "snapshot_term" => snap_term
        );

        true
    }
}
