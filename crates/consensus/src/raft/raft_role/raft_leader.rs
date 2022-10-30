use std::cmp::min;

use crate::{quorum::Quorum, raft::{DEFAULT_INITIAL_TERM, DUMMY_ID, DUMMY_INDEX, Raft, is_continuous_entries, raft_core::RaftCore, raft_tracker::{RaftManager, progress::Progress, progress_state::ProgressState, inflights::Inflights}, read_only::ReadOnlyOption, short_message}, storage::Storage};

use super::RaftRole;
use crate::{errors::*, debug, warn, trace, info, error};
use crate::prost::Message;
use crate::protos::{
    raft_conf_proto::*,
    raft_log_proto::{Entry, EntryType},
    raft_payload_proto::{Message as RaftMessage, MessageType::*},
};

/// Traits for raft leader
pub trait LeaderRaft {
    // Broadcast heartbeat to followers as Leader.
    fn ping(&mut self);

    /// Tick this node do heartbeat as Leader, the leader 
    /// will check quorum first (if configured), makesure its 
    /// still leader, then generate some heartbeat msg to ready.
    fn tick_heartbeat(&mut self) -> bool;

    fn broadcast_heartbeat(&mut self);

    /// Leader peer broadcast conf change proposal of cluster to followers.
    fn broadcast_cluster_conf_change(&mut self);

    fn broadcast_heartbeat_with_ctx(&mut self, context: Option<Vec<u8>>);

    fn broadcast_append(&mut self);

    /// Send an Append message with entries (if any) to specific follower. 
    /// The entries prepare to send depends on this progress's `next_index`.
    /// Return true if progress of `to_voter` exists, but that not means Append
    /// must be sent, for example the progress is "paused", in this case,
    /// there has nothing to send.
    fn send_append(&mut self, to_voter: u64) -> bool;

    fn in_lease(&self) -> bool;

    fn process(&mut self, message: RaftMessage) -> Result<()>;

    fn should_broadcast_commit(&self) -> bool;

    /// Try to update the quorum_commit to current leader's raft_log and progress
    /// then broadcast to other peers if update success
    fn try_commit_and_broadcast(&mut self) -> bool {
        self.try_commit(true)
    }
    
    /// Try to update the quorum_commit to current leader's raft_log and progress, 
    /// then broadcast updated commit to other peers if `should_broadcast`
    /// ## When commit?
    /// In this situation, assume progress of all followers in state machine is: <br/>
    /// #### Before broadcast `MsgAppend` index 3.
    /// matched (index): `[1,2,2,2,3]`, quorum's index is 2 (because majority is 2) <br/>
    /// #### After received `MsgAppendResponse`
    /// * After recv peer 1 (and send 2,3 to peer 1): `[3,2,2,2,3]`, then quorum's index still 2, shouldn't update commit.
    /// * After recv peer 2 (accept): `[3,3,2,2,3]`, then quorum's index become 3, should update commit.
    /// * After recv peer 3 (accept): `[3,3,3,2,3]`, then quorum's index still 3, shouldn't update commit.
    /// * And so forth...
    ///
    /// ## Params
    /// * ***should_broadcast***: to control weather to broadcast updated commit
    /// ## Returns
    /// * ***has_updated***: true if both update raft_log and progress success
    fn try_commit(&mut self, should_broadcast: bool) -> bool;

    fn check_quorum_active(&mut self) -> bool;

    fn is_log_committed_to_current_term(&self) -> bool;

    /// Reduce the size of `reduce_entries` from `uncommitted_state`
    fn reduce_uncommitted_size(&mut self, entries_reduce: &[Entry]);
}

impl<STORAGE: Storage> LeaderRaft for Raft<STORAGE> {
    fn ping(&mut self) {
        if self.current_raft_role == RaftRole::Leader {
            self.broadcast_heartbeat();
        }
    }

    fn tick_heartbeat(&mut self) -> bool {
        self.heartbeat_elapsed += 1;
        self.election_elapsed += 1;

        let mut has_ready = false;
        if self.election_elapsed >= self.election_timeout {
            self.election_elapsed = 0;
            // step 1: check if quorum still active (majority followers are recent_active)
            if self.check_quorum {
                let msg = short_message(Some(self.id), DUMMY_ID, MsgCheckQuorum);
                has_ready = true;
                let _ = self.process(msg);
            }
            if self.current_raft_role == RaftRole::Leader && self.lead_transferee.is_some() {
                // I'm still leader, abort transfer leadership.
                self.core.abort_leader_transfer()
            }
        }

        if self.current_raft_role != RaftRole::Leader {
            // check quorum failed, I'm not leader anymore.
            return has_ready;
        }

        // step 2: then try broadcasting heartbeat if I'm still Leader.
        if self.heartbeat_elapsed >= self.heartbeat_timeout {
            self.heartbeat_elapsed = 0;
            has_ready = true;
            let msg = short_message(Some(self.id), DUMMY_ID, MsgBeat);
            let _ = self.process(msg);
        }
        has_ready
    }

    fn broadcast_heartbeat(&mut self) {
        let last_ctx = self.read_only.last_pending_read_ctx();
        self.broadcast_heartbeat_with_ctx(last_ctx)
    }

    fn broadcast_heartbeat_with_ctx(&mut self, context: Option<Vec<u8>>) {
        let self_id = self.id;
        let core = &mut self.core;
        let messages = &mut self.messages;
        self.tracker
            .iter_mut()
            .filter(|&(id, _)| *id != self_id)
            .for_each(|(id, progress)| core.send_heartbeat(*id, progress, messages, context.clone()));
    }

    fn broadcast_append(&mut self) {
        let my_id = self.id;
        let endpoint = &mut self.core;
        let msg_box = &mut self.messages;
        self.tracker.iter_mut()
            .filter(|&(peer_id, _)| *peer_id != my_id)
            .for_each(|(peer_id, progress)| endpoint.send_append(*peer_id, progress, msg_box));
    }

    fn send_append(&mut self, to_peer: u64) -> bool {
        if let Some(progress) = self.tracker.get_mut(to_peer) {
            self.core.send_append(to_peer, progress, &mut self.messages);
            true
        } else {
            false
        }
    }

    fn in_lease(&self) -> bool {
        self.current_raft_role == RaftRole::Leader && self.check_quorum
    }

    fn process(&mut self, mut message: RaftMessage) -> Result<()> {
        match message.msg_type() {
            MsgBeat => self.broadcast_heartbeat(),
            MsgCheckQuorum => {
                if !self.check_quorum_active() {
                    warn!("stepped down to follower since quorum is not active");
                    let term = self.term;
                    self.become_follower(term, DUMMY_ID);
                }
            }
            // receive and handle proposal, then broadcast `MsgAppend`
            MsgPropose => {
                return self.handle_proposal(&mut message);
            },
            MsgReadIndex => {
                if !self.is_log_committed_to_current_term() {
                    return Ok(());
                }
                
                let read_index = self.raft_log.quorum_committed;
                let self_id = self.id;
                
                // only 1 peer (Leader) in quorum, then just handle it local and send response.
                if self.tracker().is_standalone_mode() {
                    if let Some(read_index_resp) = self.handle_ready_read_index(message, read_index) {
                        self.core.send_to_mailbox(read_index_resp, &mut self.messages);
                    }
                    return Ok(());
                }
                
                match self.read_only_option() {
                    ReadOnlyOption::Safe => {
                        // otherwise if in safe mode, broadcast heartbeat first, ensure I'm still leader,
                        // then collect read context from heartbeat.
                        let context = message.entries[0].data.to_vec();
                        self.core.read_only.add_request(read_index, message, self_id);
                        self.broadcast_heartbeat_with_ctx(Some(context));
                    },
                    ReadOnlyOption::LeaseBased => {
                        // or if in lease read, handle it and response immediately.
                        if let Some(read_index_resp) = self.handle_ready_read_index(message, read_index) {
                            self.core.send_to_mailbox(read_index_resp, &mut self.messages)
                        }
                    }
                }
            },
            // process the response type messages
            MsgAppendResponse => self.handle_append_response(&message),
            MsgHeartbeatResponse => self.handle_heartbeat_response(&message),
            MsgSnapStatus => self.handle_snapshot_status(&message),
            MsgUnreachable => self.handle_unreachable(&message),
            MsgTransferLeader => self.handle_transfer_leader(&message),
            _ => {}
        }
        Ok(())
    }

    #[inline]
    fn should_broadcast_commit(&self) -> bool {
        !self.is_skip_bcast_commit() || self.has_pending_conf()
    }

    fn try_commit(&mut self, should_broadcast: bool) -> bool {
        if self.maybe_commit() {
            trace!("should leader broadcast committed? {:?}", should_broadcast);
            if should_broadcast {
                self.broadcast_append()
            }
            return true;
        }
        false
    }

    fn check_quorum_active(&mut self) -> bool {
        let self_id = self.id;
        self.mut_tracker().quorum_recently_active(self_id)
    }

    fn is_log_committed_to_current_term(&self) -> bool {
        self.raft_log.match_term(self.raft_log.quorum_committed, self.term)
    }

    fn broadcast_cluster_conf_change(&mut self) {

        let self_id = self.id;
        // try to commit if more entries has been committed, if in this case, 
        // the latest commit should be broadcast (append) to all followers.
        if !self.try_commit_and_broadcast() {
            // otherwise still probe the newly added replicas.
            let core = &mut self.core;
            let msg_box = &mut self.messages;
            self.tracker
                .iter_mut()
                .filter(|&(id, _)| *id != self_id)
                .for_each(|(peer_id, progress)| { 
                    core.try_send_append(*peer_id, progress, msg_box, false);
                });
        }

        if let Some(ctx) = self.read_only.last_pending_read_ctx() {
            let tracker = &self.tracker;
            if self.core
                   .read_only
                   .recv_ack(self_id, &ctx)
                   .map_or(false, |acks| tracker.has_reached_quorum(acks))
            {
                for rs in self.core.read_only.advance(&ctx) {
                    if let Some(read_index_resp) = self.handle_ready_read_index(rs.req, rs.index) {
                        self.core.send_to_mailbox(read_index_resp, &mut self.messages);
                    }
                }
            }
        }

        if self.lead_transferee.map_or(false, |peer_id| !self.tracker.quorum.contain_voter(peer_id))
        {
            self.abort_leader_transfer();
        }
    }

    fn reduce_uncommitted_size(&mut self, entries_reduce: &[Entry]) {
        if self.current_raft_role != RaftRole::Leader {
            return;
        }

        if !self.try_decr_uncommitted_size(entries_reduce) {
            warn!(
                "try to reduce uncommitted size less than 0, first index of pending ents is {}",
                entries_reduce[0].index
            );
        }
    }
}

/// Leader peer represent a abstract endpoint of Leader in a Raft quorum.
/// It's act as a leader to send proposal entries, send snapshot etc..
trait LeaderPeer {
    /// Try sending the entries (if any) from specific (New)Leader's progress to specific peers. <br/>
    /// Noting that: the message (`MsgSnapshot` or `MsgAppend`) will be stashed to raft.messages until it's been
    /// send to real IO via `raft_node.take_message`
    /// ## Params
    /// * to_peer: mark this message send to which peer
    /// * progress: the entries will be fetched from specific peer's progress and
    /// send to `to_peer`
    /// * msg_box: the generated message will be stashed in it.
    fn send_append(&mut self, to_peer: u64, progress: &mut Progress, msg_box: &mut Vec<RaftMessage>) {
        let _ = self.try_send_append(to_peer, progress, msg_box, true);
    }

    fn send_heartbeat(&mut self, to_peer: u64, progress: &mut Progress, msg_box: &mut Vec<RaftMessage>, context: Option<Vec<u8>>);

    /// Try sending append entries to `to_peer` if that peer is not in state `Replicate`.
    /// * If that peer's previous index of `progress.next` even not in leader's raft_log
    /// (maybe log_entry in store has been compacted before), then leader will send `MsgSnapshot`
    /// to the peer, and change that peer's state to `Snapshot`.
    /// * If find some entries from that peer's progress.next -1 to applied, then continuous 
    /// sending entries to it.
    /// 
    /// ## Params
    /// * to_peer: mark this message send to which peer
    /// * progress: the entries will be fetched from specific peer's progress and
    /// send to `to_peer`
    /// * msg_box: the generated message will be stashed in it.
    /// * allow_empty: to controls whether messages with no entries will be sent
    /// ("empty" messages are useful to convey updated Commit indexes, but
    /// are undesirable when we're sending multiple messages in a batch).
    /// 
    /// ## Returns
    /// * true if: found and send some entries in [MsgAppend](protos::raft_payload_proto::MessageType::MsgAppend)
    /// 
    /// * false if:
    ///     * nothing to send and not allow empty
    ///     * progress.is_paused()
    ///     * found and send snapshot to follower
    /// 
    /// ## Prossibly Generated
    /// ### Message
    /// `MsgSnapshot` or `MsgAppend`
    fn try_send_append(&mut self, to_peer: u64, progress: &mut Progress, msg_box: &mut Vec<RaftMessage>, allow_empty: bool) -> bool;

    /// Try batching allow us to `append` already exists 'entries' to to_peer's message.entries
    /// ## Params
    /// * to_peer: mark this message send to which peer
    /// * progress: the entries will be fetched from specific peer's progress and
    /// send to `to_peer`
    /// * msg_box: the generated message will be stashed in it.
    /// * entries: Already exists entries (from unstable & stable store)
    fn try_batching(&mut self, to_peer: u64, progress: &mut Progress, msg_box: &mut [RaftMessage], entries: &mut Vec<Entry>) -> bool;

    /// Prepare to send snapshot before stash send message, this step always
    /// called in `try_send_append` if in these cases:
    /// ### Send Snapshot Cases
    /// * Still exists `pending_request_snapshot` in `progress`
    /// * Failed to get index or entries to append from current peer's raft_log
    /// ### Message Generated
    /// `MsgSnapshot` might be generated in normal case, 
    /// then `progress` state machine will be enter `snapshot` state
    fn prepare_send_snapshot(&mut self, to_peer: u64, progress: &mut Progress, message: &mut RaftMessage) -> bool;

    /// Prepare to set some payload to message before stash message to `raft.messages`
    /// This action will push last entry's index to `progress.inflights`
    fn prepare_send_append(&mut self, message: &mut RaftMessage, progress: &mut Progress, term: u64, entries: Vec<Entry>);
}

impl<STORAGE: Storage> LeaderPeer for RaftCore<STORAGE> {
    fn send_heartbeat(
        &mut self,
        to_peer: u64,
        progress: &mut Progress,
        msg_box: &mut Vec<RaftMessage>,
        context: Option<Vec<u8>>,
    ) {
        let mut heartbeat = short_message(None, to_peer, MsgHeartbeat);
        // make sure to send stable committed index
        heartbeat.commit = min(progress.match_index, self.raft_log.quorum_committed);
        if let Some(ctx) = context {
            heartbeat.context = ctx;
        }
        self.send_to_mailbox(heartbeat, msg_box);
    }

    fn try_send_append(
        &mut self,
        to_peer: u64,
        progress: &mut Progress,
        msg_box: &mut Vec<RaftMessage>,
        allow_empty: bool,
    ) -> bool {
        if progress.is_paused() {
            trace!(
                "Skipping sending to peer: {peer}, it's paused", peer = to_peer;
                "progress" => ?progress
            );
            return false;
        }
        let mut message = RaftMessage::default();
        message.to = to_peer;

        // Detect if there still exists pending of request snapshot
        if progress.still_pending_snapshot_request() {
            if !self.prepare_send_snapshot(to_peer, progress, &mut message) {
                return false;
            }
        } else {
            // fetch entries of specific follower from it's next_index to end (or max_msg_size default to 1)
            let entries_to_send = self
                .raft_log
                .entries_remain(progress.next_index, self.max_msg_size);
            // not allow send empty and found nothing to send.
            if !allow_empty && entries_to_send.as_ref().ok().map_or(true, |e| e.is_empty()) {
                return false;
            }
            let last_term = self.raft_log.term(progress.next_index - 1);
            match (last_term, entries_to_send) {
                // if has some entries to send to specific follower, then send (or keep in batch)
                // this means that follower in Probe state.
                (Ok(last_term), Ok(mut entries_to_send)) => {
                    if self.try_batching(to_peer, progress, msg_box, &mut entries_to_send) {
                        return true;
                    }
                    self.prepare_send_append(&mut message, progress, last_term, entries_to_send);
                }
                _ => {
                    // even follower's matched index not in leader's raft_log (both unstable and stable)
                    // means that follower in Snapshot state, need sync with snapshot first.
                    if !self.prepare_send_snapshot(to_peer, progress, &mut message) {
                        return false;
                    }
                }
            }
        }
        self.send_to_mailbox(message, msg_box);
        true
    }

    fn try_batching(
        &mut self,
        to_peer: u64,
        progress: &mut Progress,
        msg_box: &mut [RaftMessage],
        entries: &mut Vec<Entry>,
    ) -> bool {
        let mut is_batching = false;
        for msg in msg_box {
            if !(msg.to == to_peer && msg.msg_type() == MsgAppend) {
                continue;
            }
            if !entries.is_empty() {
                if !is_continuous_entries(msg, entries) {
                    return is_batching;
                }
                msg.entries.append(entries);
                let last_index = msg.entries.last().unwrap().index;
                progress.push_inflight(last_index);
            }
            msg.commit = self.raft_log.quorum_committed;
            is_batching = true;
            break;
        }

        is_batching
    }

    fn prepare_send_snapshot(
        &mut self,
        to_peer: u64,
        progress: &mut Progress,
        message: &mut RaftMessage,
    ) -> bool {
        if !progress.recent_active {
            debug!(
                "ignore sending snapshot to {} since it's not recently active",
                to_peer
            );
            return false;
        }
        message.set_msg_type(MsgSnapshot);
        let my_snapshot = self.raft_log.snapshot(progress.pending_request_snapshot());

        if let Err(err) = my_snapshot {
            if err == Error::Store(StorageError::SnapshotTemporarilyUnavailable) {
                debug!(
                    "failed to send snapshot to {} because snapshot is temporarily unavailable",
                    to_peer
                );
                return false;
            }
            panic!("unexpected error: {:?}", err);
        }

        let my_snapshot = my_snapshot.unwrap();
        let meta = my_snapshot.get_metadata();
        let (snapshot_index, snapshot_term) = (meta.index, meta.term);
        if snapshot_index == DUMMY_INDEX {
            panic!("require for non-empty snapshot");
        }

        debug!(
            "[first index: {first_index}, quorum committed: {committed}] sent snapshot[index: {snapshot_index}, term: {snapshot_term}] to {to}",
            first_index = self.raft_log.first_index().unwrap(),
            committed = self.raft_log.quorum_committed,
            snapshot_index = snapshot_index,
            snapshot_term = snapshot_term,
            to = to_peer;
            "progress" => ?progress,
        );
        message.snapshot = Some(my_snapshot);
        progress.enter_snapshot(snapshot_index);

        debug!("paused sending replication messages to {}", to_peer);
        true
    }

    fn prepare_send_append(
        &mut self,
        message: &mut RaftMessage,
        progress: &mut Progress,
        term: u64,
        entries: Vec<Entry>,
    ) {
        message.set_msg_type(MsgAppend);
        message.log_term = term;
        message.index = progress.next_index - 1;
        message.entries = entries.into();
        message.commit = self.raft_log.quorum_committed;
        if !message.entries.is_empty() {
            // then mark the latest proposal entries index (last one) to inflights
            let latest_appended_index = message.entries.last().unwrap().index;
            progress.push_inflight(latest_appended_index);
        }
    }
}

trait LeaderHandler {
    fn handle_proposal(&mut self, message: &mut RaftMessage) -> Result<()>;

    /// ## Description
    /// Recv and handle append response, then maybe update statemachine of 
    /// append responder (follower)
    fn handle_append_response(&mut self, message: &RaftMessage);

    /// ## Description
    /// Handle message with type `MsgHeartbeatResponse` after invoke `process`.
    /// ## Possibly generated
    /// ### Message
    /// * `MsgReadIndexResp` when step message with context `read-only`
    /// * `MsgSnapshot` or `MsgAppend` if ack follower raft_log is not fresh
    /// ### Entries
    /// 
    fn handle_heartbeat_response(&mut self, message: &RaftMessage);

    /// Leader receive and handle `MsgTransferLeader`, might from remote follower forward 
    /// or from local api call.
    fn handle_transfer_leader(&mut self, message: &RaftMessage);

    /// Leader receive snapshot status from follower and handle it. 
    /// Only if this follower is in Snapshot state, then update progress 
    /// of this follower to Probe.
    /// And if snapshot has not been 
    /// [Finish](crate::raft_node::SnapshotStatus::Finish),
    /// then clear the `pending_snapshot` record.
    fn handle_snapshot_status(&mut self, message: &RaftMessage);

    fn handle_unreachable(&mut self, message: &RaftMessage);

    /// Try to update the quorum_commit to current leader's raft_log and progress
    /// Steps of commit:
    /// 1. Find all follower's progress `matched` index in `ProgressMap`.
    /// 2. Calculate quorum's committed index from these `matcheds`
    /// 3. If calculated `quorum_committed` larger than old `quorum_committed` in `raft_log`, then update to it.
    /// ## Returns
    /// * true if both update raft_log and progress success
    fn maybe_commit(&mut self) -> bool;

    /// This msg always use to notify replicated follower (candidate)
    /// to campaign election after handle transfer_leader.
    fn send_timeout_now(&mut self, to_peer: u64);
}

impl<STORAGE: Storage> LeaderHandler for Raft<STORAGE> {
    fn handle_proposal(&mut self, message: &mut RaftMessage) -> Result<()> {
        // message's entries should not be empty
        if message.entries.is_empty() {
            panic!("receive empty proposal from {:?}", message);
        }
        // case when current peer is not in progress tracker
        if !self.tracker.all_progress().contains_key(&self.id) {
            return Err(Error::ProposalDropped(format!("this peer {:?} not in state-machine", self.id)));
        }
        // case when leadership transfering, proposal dropped
        if self.lead_transferee.is_some() {
            let reason = format!("[term {:?}] transfer leadership to {:?} is in progress", self.term, self.lead_transferee.unwrap());
            debug!("{:?}; droping proposal", reason);
            return Err(Error::ProposalDropped(reason));
        }

        // determine if there exists some config change in proposal and handle it
        for (i, entry) in message.entries.iter_mut().enumerate() {
            if entry.entry_type() != EntryType::EntryConfChange {
                continue;
            }
            // let mut conf_change;
            let mut conf_change = BatchConfChange::default();
            // decode config data from entry.data, developer can attach config at an entry
            match conf_change.merge(entry.data.as_slice()) {
                Err(err) => {
                    error!("failed to decode conf change data from message.entries, see:"; "error" => ?err);
                    return Err(Error::ProposalDropped(format!("failed to decode conf change data from message.entries")));
                }
                _ => ()
            }

            let reject_reason = if self.has_pending_conf() {
                Some("possible unapplied conf change")
            } else {
                let already_joint: bool = self.tracker.quorum.already_joint();
                let want_leave: bool = conf_change.changes.is_empty();
                let mut reject_reason = None;
                if already_joint && !want_leave {
                    reject_reason = Some("must transition out of joint config first");
                } else if !already_joint && want_leave {
                    reject_reason = Some("not in joint state, refusing empty conf change");
                }
                reject_reason
            };

            if reject_reason.is_none() {
                self.pending_conf_index = self.raft_log.last_index().unwrap() + i as u64;
            } else {
                // if refuse conf change, then reset entry's content
                info!(
                    "ignoring conf change"; 
                    "modification" => ?conf_change, "reason" => reject_reason, 
                    "pending index" => self.pending_conf_index, "applied" => self.raft_log.get_applied()
                );
                // ** be careful here, the conf entry maybe modified to normal
                *entry = Entry::default();
                entry.set_entry_type(EntryType::EntryNormal);
            }
        }

        // try to append entries to current raft peers's raft_log
        if !self.append_entries(&mut message.entries) {
            debug!(
                "entries are dropped due to overlimit of max uncommitted size, uncommitted_size: {}",
                self.uncommitted_size()
            );
            return Err(Error::ProposalDropped("entries are dropped due to overlimit of max uncommitted size".into()));
        }

        // Then broadcast the proposal to all followers & candidate
        self.broadcast_append();
        Ok(())
    }

    fn handle_append_response(&mut self, message: &RaftMessage) {
        let mut next_probe_index = message.reject_hint;
        if message.reject && message.log_term > DEFAULT_INITIAL_TERM {
            next_probe_index = self.raft_log.find_conflict_by_term(message.reject_hint, message.log_term).0;
        }

        let responder = message.from;
        let responder_progress = match self.tracker.get_mut(responder) {
            Some(progress) => progress,
            None => {
                debug!("not progress available for responder {}", responder);
                return;
            }
        };
        responder_progress.recent_active = true;
        responder_progress.update_committed(message.commit);
        
        if message.reject {
            debug!(
                "received msgAppend rejection";
                "reject_hint_index" => message.reject_hint,
                "reject_hint_term" => message.log_term,
                "from" => message.from,
                "index" => message.index,
            );

            if responder_progress.try_decr_to(message.index, next_probe_index, message.request_snapshot) {
                trace!(
                    "decreased progress of {}",
                    message.from;
                    "progress" => ?responder_progress,
                );
                if responder_progress.state == ProgressState::Replicate {
                    responder_progress.enter_probe();
                }
                self.send_append(responder);
            }
            // case 1: receive reject append response from a follower who's in state Probe or Snapshot,
            // then decrease it's progress matched and start to syncing with it.
            return;
        }
        let paused_before_update = responder_progress.is_paused();
        if !responder_progress.try_update(message.index) {
            // case 2: receive a out dated message index, and should not update the progress.
            return;
        }
        // then follower transfer to next state
        responder_progress.next_state(message.index, message.from);

        if self.try_commit(self.should_broadcast_commit()) {

        } else if paused_before_update {
            self.send_append(responder);
        }

        let responder_progress = self.tracker.get_mut(responder).unwrap();

        // If we have more entries to send to a not replicated follower, send as many messages as we
        // can (without sending empty messages for the commit index)
        // this action may generate more than one append msg when remained entries is more than 
        // max_msg_size in configuration
        while self.core.try_send_append(responder, responder_progress, &mut self.messages, false) {}

        if Some(responder) == self.core.lead_transferee {
            let last_index = self.core.raft_log.last_index().unwrap();
            if responder_progress.match_index == last_index {
                info!("sent MsgTimeout to {peer} after received MsgAppendResponse from it", peer = responder);

                // if this append & append_response is cause by transfer_leader,
                // then response timeout to replicated follower (candidate) and tell 
                // it to make election.
                self.send_timeout_now(responder);
            }
        }
    }

    fn handle_heartbeat_response(&mut self, message: &RaftMessage) {
        let msg_from = message.from;
        let progress = match self.tracker.get_mut(msg_from) {
            Some(pr) => pr,
            None => {
                debug!("no progress available for {}", msg_from);
                return;
            }
        };
 
        // step 1: update response follower's progress.
        progress.update_committed(message.commit);
        progress.recent_active = true;
        progress.resume();
 
        if progress.state == ProgressState::Replicate && progress.inflights.is_full() {
            progress.inflights.pop_front();
        }
 
        // step 2: if follower is not replicated, sync entries with it.
        if progress.match_index < self.core.raft_log.last_index().unwrap() || 
            progress.still_pending_snapshot_request() {
            self.core.send_append(msg_from, progress, &mut self.messages);
        }
 
        // In this case, context of message (heartbeat response) is the read_index ctx that sent by 
        // leader peer when handle forwarded read_index.
        if self.read_only.option != ReadOnlyOption::Safe || message.context.is_empty() {
            return;
        }
 
        // ReadIndex process:
        // 1 Leader recv read_index request (from itself or forwarder) and add to read_only.
        // 2 Leader bcast heartbeat with read_index ctx to voters (and save read ctx to it's read_only).
        // 3 Leader recv heartbeat response from each voter, when reached the majority, 
        // take read_index request out from read_oly.
        // 4-1 If the read_index request from Leader itself, nothing to response, 
        // read_state will be set to ready, just handle it and return. 
        // 4-2 Otherwise return read_index_response to forwarder.

        // step 3: tally read index result, response to requester if majority.
        match self.core.read_only.recv_ack(msg_from, &message.context) {
            Some(acks) if self.tracker.has_reached_quorum(acks) => (),
            _ => return,
        }
 
        // step 4: advance `pending_read_index` and 
        // handle read_index after heartbeat and collect response.
        for read_index_status in self.read_only.advance(&message.context) {
            if let Some(read_index_resp) = self.handle_ready_read_index(read_index_status.req, read_index_status.index) {
                self.core.send_to_mailbox(read_index_resp, &mut self.messages);
            }
        }
    }

    fn handle_transfer_leader(&mut self, message: &RaftMessage) {
        let msg_from = message.from;
        if self.tracker().get(msg_from).is_none() {
            debug!("no progress available for {}", msg_from);
            return;
        }

        if self.tracker.learners.contains(&msg_from) {
            debug!("ignored transferring leadership to {}", msg_from);
            return;
        }

        let lead_transferee = msg_from;
        if let Some(last_lead_transferee) = self.lead_transferee {
            if last_lead_transferee == lead_transferee {
                info!(
                    "[term {term}] transfer leadership to {lead_transferee} is in progress, ignores request \
                     to same node {lead_transferee}",
                    term = self.term,
                    lead_transferee = lead_transferee;
                );
                return;
            }
            self.abort_leader_transfer();
            info!(
                "[term {term}] abort previous transferring leadership to {last_lead_transferee}",
                term = self.term,
                last_lead_transferee = last_lead_transferee;
            );
        }
        if lead_transferee == self.id {
            debug!("already leader; ignored transferring leadership to self");
            return;
        }
        // Transfer leadership to third party.
        info!(
            "[term {term}] starts to transfer leadership to {lead_transferee}",
            term = self.term,
            lead_transferee = lead_transferee;
        );
        // Transfer leadership should be finished in one electionTimeout
        // so reset r.electionElapsed.
        self.election_elapsed = 0;
        self.lead_transferee = Some(lead_transferee);
        let progress = self.tracker.get_mut(msg_from).unwrap();
        if progress.match_index == self.core.raft_log.last_index().unwrap() {
            self.send_timeout_now(lead_transferee);
            debug!(
                "sends MsgTimeoutNow to {lead_transferee} immediately as {lead_transferee} already has up-to-date log",
                lead_transferee = lead_transferee;
            );
        } else {
            self.core
                .send_append(lead_transferee, progress, &mut self.messages);
        }
    }

    fn handle_snapshot_status(&mut self, message: &RaftMessage) {
        let msg_from = message.from;
        let progress = match self.tracker.get_mut(msg_from) {
            Some(pr) => pr,
            None => { 
                debug!("no progress available for {}", msg_from);
                return;
            }
        };
    
        if progress.state != ProgressState::Snapshot {
            return;
        }
    
        if message.reject {
            progress.snapshot_failure();
            progress.enter_probe();
            debug!(
                "snapshot failed, resumed sending replication messages to {from}",
                from = msg_from;
                "progress" => ?progress,
            );
        } else {
            progress.enter_probe();
            debug!(
                "snapshot succeeded, resumed sending replication messages to {from}",
                from = msg_from;
                "progress" => ?progress,
            );
        }
    
        // stop appending to this probe voter (follower).
        progress.pause();
        // then clear pending request snapshot state.
        progress.clear_pending_snapshot_request();
    }
    
    fn handle_unreachable(&mut self, message: &RaftMessage) {
        let msg_from = message.from;
        let progress = match self.tracker.get_mut(msg_from) {
            Some(pr) => pr,
            None => { 
                debug!("no progress available for {}", msg_from);
                return;
            }
        };
    
        if progress.state == ProgressState::Replicate {
            progress.enter_probe();
        }
        
        debug!(
            "failed to send message to {from} because it is unreachable",
            from = msg_from;
            "progress" => ?progress,
        );
    }

    fn maybe_commit(&mut self) -> bool {
        let (quorum_committed, _) = self.mut_tracker().quorum_committed_index();
        // try to update commit index of raft_log to quorum committed
        if self.core.raft_log.maybe_commit(quorum_committed, self.core.term) {
            // then update commit index of current peer's progress
            // self.raft_log.quorum_committed is equal to quorum_committed here
            let (self_id, quorum_committed) = (self.id, self.raft_log.quorum_committed);
            self.mut_tracker().get_mut(self_id).unwrap().update_committed(quorum_committed);
            return true;
        }
        // only if success update raft_log and progress of current peer is true
        false
    }

    fn send_timeout_now(&mut self, to_peer: u64) {
        let msg = short_message(None, to_peer, MsgTimeoutNow);
        self.core.send_to_mailbox(msg, &mut self.messages);
    }
}
