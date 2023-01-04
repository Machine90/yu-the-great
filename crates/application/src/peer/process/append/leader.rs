use std::fmt::{Debug, Display};
use std::time::{Duration, Instant};

use crate::mailbox::api::MailBox;
use crate::peer::{pipeline::async_pipe::Pipelines};
use crate::peer::{Core, Peer, process::{msgs}};
use common::protocol::proposal::Proposal;
use common::protos::raft_log_proto::Snapshot;
use consensus::raft_node::SnapshotStatus;
use consensus::raft_node::raft_functions::RaftFunctions;
use consensus::raft_node::raft_process::RaftProcess;
use crate::vendor::prelude::*;
use crate::ConsensusError::Nothing;

use crate::torrent::{runtime};

use crate::{
    NodeID,
    tokio::{sync::mpsc::{channel, Sender, error::SendError}, time::timeout},
    RaftMsg, RaftResult, RaftMsgType::{MsgSnapshot, MsgAppendResponse}
};

/// Commit contents: 
/// * commit: latest quorum's commit index value.
/// * commit_msg: to broadcast to follower tell them commit to latest index.
/// * supplement: after apply_conf_change, leader may generate some append to new added node.
pub type CommitContent = (u64, Option<Vec<RaftMsg>>, Option<Vec<RaftMsg>>);

enum NextState {
    /// once a proposal has been commit, then step in this case.
    Commit(CommitContent),
    /// Keep append to target peer (follower)
    Append(Vec<RaftMsg>),
    End,
}

impl Display for NextState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match &self {
            NextState::Commit((c, bcast, _)) => format!("commit {:?} and should bcast? {:?}", c, bcast.is_some()),
            NextState::Append(append) => format!("append {:?} msgs to probe", append.len()),
            NextState::End => format!("end of append"),
        };
        write!(f, "{:?}", msg)
    }
}

impl Debug for NextState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Commit((idx, bcast, supplement)) => {
                let bcast_cnt = bcast.as_ref().map(|commits| commits.len()).unwrap_or(0);
                let append_to = supplement.as_ref().map(|appends| appends.iter().map(|m| m.to).collect()).unwrap_or(vec![]);
                f.debug_tuple(
                    format!("Commit {:?} with bcast commits {:?} and replicate entries to probes {:?}", *idx, bcast_cnt, append_to).as_str()
                ).finish()
            },
            Self::Append(appends) => {
                let append_to: Vec<_> = appends.iter().map(|m| m.to).collect();
                f.debug_tuple(format!("Probe followers: {:?}", append_to).as_str()).finish()
            },
            Self::End => write!(f, "End"),
        }
    }
}

/// As a Leader to send append (or snapshot).
impl Peer {
    /// ## Definition
    /// broadcast messages in type [MsgAppend](protos::raft_payload_proto::MessageType::MsgAppend)
    /// or [MsgSnapshot](protos::raft_payload_proto::MessageType::MsgSnapshot)
    /// as [Leader](consensus::raft::raft_role::RaftRole::Leader).
    /// The behavior of broadcast is return immediately if majority peers:
    /// * **Phase 1**: Leader has committed log entry and majority (n / 2) follower has persist log entry.
    /// * **Phase 2**: Leader and majority Follower has committed log entry.
    /// ### Returns
    /// * **Ok**: true if log entry has been committed after broadcast.
    pub(crate) async fn broadcast_append(&self, append: Vec<RaftMsg>, timeout_dur: Duration) -> RaftResult<Proposal> {
        let total = append.len();
        let mut pipelines = Pipelines::new(self.mailbox.clone());
        let core = self.core.clone();

        pipelines.broadcast_with_async_cb(append, move |follower, append_resp| {
            let core = core.clone();
            crate::trace!("follower-{:?} has replied for append", follower);
            async move {
                core._handle_append_response(append_resp?).await
            }
        });

        let all = pipelines.join(total).await;
        let mut leader_commit = None;
        let mut probes = vec![];

        // tally append result from each follower.
        for (follower, next) in all {
            if let Err(e) = next {
                warn!(
                    "failed to broadcast append to {:?} due to: {:?}",
                    follower, e
                );
                continue;
            }
            let next = next.unwrap();
            match next {
                NextState::Commit((index, commit_mails, supplement)) => {
                    leader_commit = Some((index, commit_mails, supplement));
                }
                NextState::Append(appends) => {
                    probes.push((follower, appends));
                }
                NextState::End => continue,
            }
        }

        if let Some((idx, commit_mails, supplement)) = leader_commit {
            // if commit in this broadcast pipeline, then return immediately
            if let Some(commit_mails) = commit_mails {
                self.broadcast_commit(commit_mails).await;
            }
            self._sync_with_probes(probes, None).await;
            self.send_append_fut(supplement, "broadcast and commit immediately");
            return Ok(Proposal::Commit(idx));
        } else {
            // otherwise waiting for quorum's commit for timeout.
            // assert!(probes.len() > 0, "there has not poller when append");
            if probes.len() <= 0 {
                return Ok(Proposal::Pending);
            }
            let (committer, mut poller) = channel(probes.len());
            self._sync_with_probes(probes, Some(committer)).await;
            let timeout_task = timeout(timeout_dur, poller.recv());
            let try_recv = timeout_task.await;
            if let Err(_) = try_recv {
                warn!("proposal timeout in {:?}, don't be afriad commit will broadcast as normal", timeout_dur);
                return Ok(Proposal::Timeout);
            }

            match try_recv.unwrap() {
                Some((idx, commit_msgs, appends)) => {
                    if let Some(commit_msgs) = commit_msgs {
                        self.broadcast_commit(commit_msgs).await;
                    }
                    self.send_append_fut(appends, "broadcast and commit append delay");
                    return Ok(Proposal::Commit(idx));
                }
                None => (),
            }
        }
        Ok(Proposal::Pending)
    }

    /// send supplement appends to specific followers async. 
    /// this is difference from broadcast.
    #[inline]
    pub fn send_append_fut(&self, appends: Option<Vec<RaftMsg>>, hint: &'static str) {
        if let Some(appends) = appends {
            let core = self.core.clone();
            runtime::spawn(async move {
                // do it asynchronized
                let total = appends.len();
                let timer = Instant::now();
                let ne = core._try_appending(appends).await;
                trace!(
                    "finish syncing {:?} appends when {}, total elapsed: {:?}ms, next: {:?}", 
                    total, hint, timer.elapsed().as_millis(), ne
                );
            });
        }
    }

    /// Send snapshot to specific follower.
    pub async fn send_snapshot(&self, mut snapshot: RaftMsg) {
        assert_eq!(snapshot.msg_type(), MsgSnapshot, "unexpected snapshot msg type!");
        let to = snapshot.to;
        info!("start syncing snapshot msg to: {:?}", to);
        let timer = Instant::now();

        let ctx = self.get_context(true).await;
        let prepared = self.coprocessor_driver
            .before_send_snapshot(&ctx, snapshot.get_snapshot_mut()).await;
        if let Err(e) = prepared {
            crate::error!(
                "failure to prepare the {snapshot}, the msg will not to send, reason is: {:?}", e
            );
            return;
        }
        let next = self._try_appending(vec![snapshot]).await;
        trace!(
            "finish synced snapshot with: {:?}, total elapsed: {:?}ms, next state: {:?}", 
            to, timer.elapsed().as_millis(), next
        );
    }

    /// Leader peer receive snapshot reply from follower then 
    /// send snapshot items to target follower.
    pub async fn handle_snapshot_reply(&self, snapshot_reply: Snapshot) -> RaftResult<()> {
        let ctx = self.get_context(false).await;
        // then do really start to send items by reply
        self.coprocessor_driver.do_send_snapshot(&ctx, snapshot_reply).await?;
        Ok(())
    }

    /// Leader may received append response directly when follower request snapshot 
    /// by sending a reject message with type `MsgAppendResponse`.
    pub async fn handle_append_response(&self, mut append_response: RaftMsg) {
        while let NextState::Append(ref appends) = self
            ._handle_append_response(append_response)
            .await
            .unwrap_or(NextState::End) 
        {
            // keep sending append to target follower until nothing to append.
            let resp = self._do_send(appends.to_owned()).await;
            if resp.is_none() {
                break;
            }
            append_response = resp.unwrap();
        }
    }

    async fn _sync_with_probes(
        &self,
        appends: Vec<(u64, Vec<RaftMsg>)>,
        notifier: Option<Sender<CommitContent>>,
    ) {
        let mut probers = vec![];
        for (to, appends) in appends {
            let committer = notifier.clone();
            let core = self.core.clone();
            let prober = runtime::spawn(async move {
                // step 1: keep sending appends (and snapshots) until the end (nothing to sync)
                let next_step = core._try_appending(appends).await;

                // step 2: if required for response when commit, then try notifying this event.
                if let Some(committer) = committer {
                    let _ = match next_step {
                        NextState::Commit((idx, commit_mails, appends)) => {
                            let try_commit = committer.send((idx, commit_mails, appends)).await;
                            debug!("try sending out commit {:?} when advance append response from {:?}", &try_commit, to);

                            // fallback policy
                            if let Err(e) = try_commit {
                                let SendError((index, commit_msg, appends)) = e;
                                warn!(
                                    "broadcast append (of commit: {:?}) listener has been closed early, don't worry, 
                                    commit will be broadcast as normal", index
                                );
                                if let Some(commit_msg) = commit_msg {
                                    core.broadcast_commit(commit_msg).await;
                                }
                                if let Some(appends) = appends {
                                    core._try_appending(appends).await;
                                }
                            }
                        }
                        NextState::Append(_) => unreachable!(),
                        NextState::End => (), // do nothing
                    };
                }
            });
            probers.push(prober);
        }
        for prober in probers {
            let _ = prober.await;
        }
    }
}

/// As a Leader to receive append(or snapshot).
impl Peer {

    /// Only update state of reporter's progress and clear `pending_request_snapshot` state.
    #[inline]
    pub async fn recv_report_snapshot(&self, reporter: NodeID, status: SnapshotStatus) {
        self.wl_raft().await.report_snapshot(reporter, status);
    }
}

impl Core {
    /// broadcast all follower tell them to commit their entries to committed index,
    /// then step and advance to update progress of these follower via their
    /// append_response, but don't consinuous sending append to non-replicated one.
    pub(crate) async fn broadcast_commit(&self, msgs: Vec<RaftMsg>) -> bool {
        let total = msgs.len();
        let mut notifies = Vec::with_capacity(total);
        for append in msgs {
            let mailbox = self.mailbox.clone();
            let notify = runtime::spawn(async move {
                if let Err(e) = mailbox.send_append_async(append).await {
                    crate::warn!("{:?}", e);
                    false
                } else {
                    true
                }
            });
            notifies.push(notify);
        }
        let mut maybe_success = 0;
        for notify in notifies {
            let notified = notify.await.unwrap_or(false);
            if notified {
                maybe_success += 1;
            }
        }
        maybe_success >= total / 2 + 1
    }

    /// This method using to send batch append messages to follower.
    async fn _try_appending(&self, messages: Vec<RaftMsg>) -> NextState {
        // otherwise send then one by one and collect last response to advance.
        let resp = self.mailbox.batch_appends(messages).await;
        if resp.is_none() {
            return NextState::End;
        }
        let mut next_step = self
            ._handle_append_response(resp.unwrap())
            .await
            .unwrap_or(NextState::End);

        while let NextState::Append(ref appends) = &next_step {
            // keep transfering remain entries to probe
            let resp = self._do_send(appends.to_owned()).await;
            if resp.is_none() {
                return NextState::End;
            }
            next_step = self._handle_append_response(resp.unwrap()).await.unwrap_or(NextState::End);
        }
        next_step
    }

    /// do really send append or snapshot to probers.
    async fn _do_send(&self, mut appends: Vec<RaftMsg>) -> Option<RaftMsg> {
        let cp_driver = self.coprocessor_driver.clone();
        let ctx = self.get_context(true).await;

        // step 1: enhance snapshot msg by coprocessor before send to mailbox first.
        let mut snaps = vec![];
        for snap in appends.iter_mut().filter(|msg| msg.msg_type() == MsgSnapshot) {
            let prepared = cp_driver.before_send_snapshot(
                &ctx, 
                snap.get_snapshot_mut()
            ).await;
            if let Err(e) = prepared {
                crate::error!(
                    "failure to prepare the {snap}, reason is: {:?}", e
                );
                continue;
            }
            snaps.push(snap.clone());
        }
        
        // step 2: begin send batch to mailbox.
        let resp = self.mailbox.batch_appends(appends).await;
        // step 3: if send in expected, do something in coprocessor after that.
        // e.g. do really send backup content to remote.
        if !snaps.is_empty() {
            cp_driver.after_send_snapshots(&ctx, snaps);
        }
        resp
    }

    /// Handle append response at Leader side when after received a message
    /// from a Follower. Then generate next state after judgement.
    async fn _handle_append_response(&self, append_resp: RaftMsg) -> RaftResult<NextState> {
        assert_eq!(append_resp.msg_type(), MsgAppendResponse);
        let mut wraft = self.wl_raft().await;
        let ready = wraft.step_and_ready(append_resp);
        let mut ready = match ready {
            Ok(ready) => ready,
            Err(e) if e == Nothing => {
                // it's ok, just nothing to ready after 
                // received append response from one of follower
                return Ok(NextState::End);
            },
            Err(e) => {
                return Err(e);
            }
        };
        
        // leader maybe update it's commit after majority append response with next 
        // commit value, then new entries from last 
        let applied_before = self.apply_commit_entries(
            &mut wraft, 
            ready.take_committed_entries()
        ).await;

        // try persist ready
        let commit_in_hs = self.persist_ready(&mut ready).await?;

        // if committed has been changed, commit msg will be generate at same time.
        let appends = msgs(ready.take_messages());

        let mut light_rd = wraft.advance_append(ready);
        let committed = self.persist_light_ready(&mut light_rd).await?;

        // leader apply entries after advance append
        let applied_after = self.apply_commit_entries(
            &mut wraft, 
            light_rd.take_committed_entries()
        ).await;
        
        // maybe generate some remained append msgs when Leader apply change entry 
        // before advance ready via method apply_commit_entry. 
        // e.g. append entries to new added node.
        let supplement = msgs(light_rd.take_messages());
        self._advance_apply(wraft, applied_after.or(applied_before)).await;

        let committed = committed.or(commit_in_hs);
        if let Some(commit) = &committed {
            crate::debug!("leader commit to: {:?} after majority received append", commit);
        }

        Ok(Self::_next_append_step(committed, appends, supplement))
    }

    /// Compute the next append step via given parameters
    fn _next_append_step(committed: Option<u64>, appends: Vec<RaftMsg>, supplement: Vec<RaftMsg>) -> NextState {
        // commit append messages always generated (if should_broadcast_commit) with index committed event.
        if let Some(committed) = committed {
            // only when update commit, the broadcast appends would be generated.
            let commit_mails = if appends.is_empty() {
                None
            } else {
                Some(appends)
            };
            let supplement = if supplement.is_empty() {
                None
            } else {
                Some(supplement)
            };
            NextState::Commit((committed, commit_mails, supplement))
        } else {
            if appends.is_empty() {
                // not commit and nothing to sync to this follower
                NextState::End
            } else {
                // only send append msgs to Probe follower.
                // be caution here, raft may send entries in few messages. 
                // for example, max_msg_size config to 1, and 10 entries should be 
                // send to probe follower, then 10 message will be generated.
                NextState::Append(appends)
            }
        }
    }
}
