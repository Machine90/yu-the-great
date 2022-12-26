pub mod append;
pub mod election;
pub mod heartbeat;
pub mod proposal;
pub mod read;
pub mod tick;

#[allow(unused)]
pub mod transfer_leader;

use std::collections::HashSet;
use std::{convert::TryInto, mem};

use super::{raft_group::raft_node::WLockRaft, Core};
use crate::torrent::runtime;
use crate::{
    coprocessor::listener::RaftContext,
    debug, error,
    storage::{ReadStorage, WriteStorage},
    tokio::task::JoinHandle,
    utils::drain_filter,
    RaftMsg, RaftResult,
};
use crate::{
    NodeID,
    coprocessor::{driver::ApplyState, ChangeReason},
    protos::{
        prelude::BatchConfChange,
        raft_log_proto::{Entry, EntryType, HardState},
    },
};
use common::protos::raft_conf_proto::ConfChange;
use common::protos::raft_log_proto::Snapshot;
use components::mailbox::api::MailBox;
use components::utils::endpoint_change::{ChangeSet, RefChanged, EndpointChange};
use consensus::prelude::*;

#[inline]
pub fn exactly_one_msg(raw: Vec<Vec<RaftMsg>>) -> RaftMsg {
    assert!(raw.len() == 1 && raw[0].len() == 1);
    raw[0][0].to_owned()
}

pub fn at_most_one_msg(raw: Vec<Vec<RaftMsg>>) -> Option<RaftMsg> {
    if raw.is_empty() || raw[0].is_empty() {
        None
    } else if raw[0].len() > 1 {
        panic!("more than one message required for handle");
    } else {
        Some(raw[0][0].to_owned())
    }
}

pub fn msgs(raw: Vec<Vec<RaftMsg>>) -> Vec<RaftMsg> {
    if raw.is_empty() {
        Vec::new()
    } else {
        raw[0].to_owned()
    }
}

struct Inventory(
    /// unstable entries
    Option<Vec<Entry>>,
    /// hardstate
    Option<HardState>,
);

impl Inventory {
    #[inline]
    pub fn empty() -> Self {
        Self(None, None)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_none() && self.1.is_none()
    }
}

pub(super) struct Tasks(Vec<(String, JoinHandle<()>)>);

#[allow(unused)]
impl Tasks {
    #[inline]
    pub fn add(&mut self, label: &str, task: JoinHandle<()>) {
        self.0.push((label.to_owned(), task));
    }

    #[inline]
    pub async fn join_all(self) {
        if self.0.is_empty() {
            return;
        }
        for (label, task) in self.0 {
            let joined = task.await;
            if let Err(e) = joined {
                error!("error join task of `{:?}` because: {:?}", label, &e);
            }
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn empty() -> Self {
        Tasks(Vec::new())
    }
}

impl Core {

    #[inline]
    pub async fn get_context(&self, with_detail: bool) -> RaftContext {
        let group = self.get_group_id();
        let raft = self.rl_raft().await;
        let mut ctx: RaftContext = RaftContext::from_status(group, raft.status());
        if with_detail {
            ctx.partition = Some(self.partition.read().clone());
        }
        ctx
    }

    /// ## Definition
    /// When soft state of current raft peer has been changed.
    /// For exmaple, peer attempt to election, and change it's role
    /// to (Pre)Candidate, or peer found it's not Leader anymore.
    async fn on_soft_state_change(&self, pss: &SoftState, ss: &SoftState, reason: ChangeReason) {
        let cp_driver = self.coprocessor_driver.clone();
        let peer_id = self.get_id();
        cp_driver
            .handle_soft_state_change(peer_id, pss, ss, reason).await;
    }

    /// ## Definition
    /// Find and persist hard_state and unstable entry from Ready if exists.
    /// persist process maybe in IO wait, thus we move this job to another thread
    /// and join the result.
    async fn persist_ready(&self, ready: &mut Ready) -> RaftResult<Option<u64>> {
        // ## Dangerous
        // Do not acquire raft lock (via `self.raft_group.wl_raft()` or `rl_raft()`) here.
        // it's because that raft_group.wl_raft() maybe called outside, avoid deadlock here.

        // unstable and hardstate should be persist.
        let mut inventory = Inventory::empty();

        if !ready.get_unstable_entries().is_empty() {
            inventory.0 = Some(ready.take_unstable_entries());
        }

        let mut committed = None;
        if let Some(hs) = ready.hard_state_ref() {
            let raft_state = self.write_store.raft_state()?;
            let prev_commit = raft_state.hard_state.commit;
            if hs.commit != prev_commit {
                committed = Some(hs.commit);
            }
            inventory.1 = Some(hs.clone());
        }

        if inventory.is_empty() {
            // nothing to persist
            return Ok(None);
        }

        let cp_driver = self.coprocessor_driver.clone();
        let wstore = self.write_store.clone();
        // persist action maybe blocking current async IO, it's not efficiency.
        // thus we move this action to another thread then just wait for ready.
        let persist_handle = runtime::spawn_blocking(move || {
            let Inventory(unstable, hs) = inventory;
            let group_id = wstore.get_group_id();
            if let Some(mut unstable) = unstable {
                cp_driver.before_persist_log_entry(group_id, &mut unstable);
                let try_persist = wstore.append(unstable);
                cp_driver.after_persist_log_entry(group_id, &try_persist);
                if let Err(error) = try_persist {
                    return Err(error);
                }
            }
            if let Some(hs) = hs {
                // detect some hardstate changed
                let try_update = wstore.update_hardstate(hs);
                if let Err(error) = try_update {
                    return Err(error);
                }
            }
            Ok(())
        });
        let join = persist_handle.await;
        if let Err(err) = join {
            return Err(crate::ConsensusError::Other(
                format!("failed to handle unstable entry store task, see: {:?}", err).into(),
            ));
        }
        join.unwrap()?;
        Ok(committed)
    }

    /// Some hardstate changed and some committed index.
    /// Recommand update committed index of hardstate here rather than `persist_ready`,
    /// make sure append has been advanced.
    /// ## Dangerous
    /// Do not acquire raft lock (via `self.raft_group.wl_raft()` or `rl_raft()`) here.
    /// it's because that raft_group.wl_raft() has been called outside, avoid deadlock here.
    async fn persist_light_ready(&self, light_ready: &mut LightReady) -> RaftResult<Option<u64>> {
        if let Some(commit_index) = light_ready.committed_index() {
            let store = self.write_store.clone();
            let _committed = runtime::spawn_blocking(move || store.update_commit(commit_index))
                .await
                .unwrap()?;
        }
        Ok(light_ready.committed_index())
    }

    /// Trying to apply the snnapshot from leader at this follower, maybe finish applying 
    /// in timeout limited, then reply the leader with mail `report_snap_status`.
    async fn apply_snapshot(
        &self,
        ctx: &RaftContext,
        snap_from: NodeID,
        mut snapshot: Snapshot,
    ) -> RaftResult<ApplyState> {
        // first, try apply this snapshot request to store, maybe reject if 
        // snap out of date. if approved, means this peer's log entries is expired, 
        // then just feel free to clear them.
        self.write_store.apply_snapshot(snapshot.get_metadata().clone())?;

        // step1: prepare to accept this snapshot, before that, check what
        // I need first, then reply to leader checked result.
        if self
            .coprocessor_driver
            .before_apply_snapshot(ctx, &mut snapshot)
            .await
            .is_ok()
        {
            self.mailbox
                .accepted_snapshot(snap_from, snapshot.clone())
                .await?;
        } else {
            return Ok(ApplyState::Applied(SnapshotStatus::Failure));
        }

        // step2: then do really accept snapshot items, for example blocking incoming
        // stream in a standalone thread, and get apply state from it, maybe pending.
        let apply_state = self
            .coprocessor_driver
            .do_apply_snapshot(
                ctx, 
                &snapshot, 
                self.conf().max_wait_backup_tranfer_duration()
            ).await;

        match apply_state {
            ApplyState::Applied(status) if status == SnapshotStatus::Finish => {
                // if finished receive backups, then apply to raft log.
                crate::debug!("finish transferring backup in-place, result is: {:?}", status);
            }
            _ => (),
        }
        Ok(apply_state)
    }

    /// When committed entries generated, this method would be invoked. Both
    /// normal entry and change would be handled in this method.
    #[inline]
    fn apply_commit_entry(&self, raft: &mut WLockRaft<'_>, log_entry: Vec<Entry>) -> Tasks {
        if log_entry.is_empty() {
            return Tasks::empty();
        }

        // async tasks for returns
        let mut tasks = Vec::new();

        let mut normals = log_entry;

        // TODO: replace with drain_filter in the future when it becomes stable.
        // let changes = normals.drain_filter(|ent| ent.entry_type() == EntryType::EntryConfChange).collect::<Vec<_>>();
        let changes = drain_changes(&mut normals);

        // maybe generate some empty entry when after became lease or conf_change
        // shouldn't allow apply these entries.
        // noting: allow to keep empty confchange to apply
        remove_empty_entry(&mut normals);

        let cmds = drain_cmds(&mut normals);

        let group_id = self.get_group_id();

        if !cmds.is_empty() {
            let cp_driver = self.coprocessor_driver.clone();
            let ctx = RaftContext::from_status(group_id, raft.status());
            let handle_committed_cmds = runtime::spawn(async move {
                cp_driver.apply_cmds(ctx, cmds).await;
            });
            tasks.push(("Apply commands".to_owned(), handle_committed_cmds));
        }

        if !normals.is_empty() {
            let cp_driver = self.coprocessor_driver.clone();
            let ctx = RaftContext::from_status(group_id, raft.status());
            // step 1: fetch normal entries and send to another thread to handle the commit log entry.
            let handle_log_entry = runtime::spawn(async move {
                debug!(
                    "attempt to apply {entries} logged entries",
                    entries = normals.len()
                );
                cp_driver.apply_log_entries(ctx, normals).await;
            });
            tasks.push(("Apply normal entries".to_owned(), handle_log_entry));
        }

        if changes.is_empty() {
            return Tasks(tasks);
        }
        debug!(
            "still have {changes} changes should be apply",
            changes = changes.len()
        );
        // step 2: apply changes to current raft. then collect changes
        let mut change_queue = ChangeSet::new();
        // only accept latest success confstate if there has batch conf_change
        let mut latest_conf_state = None;

        // voters incoming & outgoing before apply batch changes.
        let voters_before_apply = raft
            .store()
            .raft_state()
            .map(|state| state.conf_state.all_voters())
            .ok();

        let mut conf_changes = vec![];
        for mut batch in changes {
            let applied = raft.apply_conf_change(&batch);
            if let Err(e) = applied {
                debug!(
                    "failed to apply change: {:?} to group, reason: {:?}",
                    batch, e
                );
                continue;
            }
            conf_changes.append(&mut batch.changes);
            latest_conf_state = Some(applied.unwrap());
        }

        if voters_before_apply.is_some() && latest_conf_state.is_some() {
            let voters = latest_conf_state.as_ref().unwrap().all_voters();
            collect_changes(
                voters_before_apply.unwrap_or_default(),
                voters,
                conf_changes,
                |change| {
                    change_queue.insert(change);
                },
            );
        }

        // step 3: if distinct changes still exists, then send it to another thread
        // to handle seperated, for example, update registration, persist them etc..
        let ctx = RaftContext::from_status(group_id, raft.status());
        if !change_queue.is_empty() {
            let cp_driver = self.coprocessor_driver.clone();
            let wl_store = self.write_store.clone();
            
            let mailbox = self.mailbox.clone();
            let is_leader = ctx.role.is_leader();

            let should_connect = cp_driver
                    .handle_group_conf_change(ctx, change_queue);
            let group_info = self.group_info();

            let notifiy_changes = runtime::spawn(async move {
                if is_leader && !should_connect.is_empty() && group_info.is_some() {
                    let group_info = group_info.unwrap();
                    if should_connect.len() == 1 {
                        // most situation is to add 1 node in a propose.
                        // send this group info without any clone cost.
                        let (_, node_id) = should_connect.iter().next().unwrap();
                        mailbox.sync_with(*node_id, group_info).await;
                    } else {
                        for (_, node_id) in should_connect {
                            mailbox.sync_with(node_id, group_info.clone()).await;
                        }
                    }
                }
                if let Some(latest_conf_state) = latest_conf_state {
                    // this maybe run in a blocking scenario.
                    let _updated = wl_store.set_conf_state(latest_conf_state);
                }
            });
            tasks.push(("Apply conf changes".to_owned(), notifiy_changes));
        }
        Tasks(tasks)
    }

    async fn _advance_apply(&self, mut raft: WLockRaft<'_>, applied: Option<u64>) -> RaftContext {
        // first, update applied value.
        if let Some(applied) = applied {
            raft.advance_apply_to(applied);
        } else {
            raft.advance_apply();
        }
        // then, fetch latest status of raft.
        let ctx: RaftContext = RaftContext::from_status(self.group_id, raft.status());
        self.coprocessor_driver.after_applied(&ctx).await;
        ctx
    }
}

use crate::protos::prelude::prost::Message;
fn drain_changes(entries: &mut Vec<Entry>) -> Vec<BatchConfChange> {
    drain_filter(
        entries,
        |entry| entry.entry_type() == EntryType::EntryConfChange,
        |mut entry| {
            let content = mem::take(&mut entry.data);
            // noting: maybe generate an empty entry with type `EntryConfChange`
            // when `auto_leave` in `commit_apply`
            if content.is_empty() {
                Some(BatchConfChange::default())
            } else {
                BatchConfChange::decode(&content[..]).ok()
            }
        },
    )
}

fn drain_cmds(entries: &mut Vec<Entry>) -> Vec<Vec<u8>> {
    drain_filter(
        entries,
        |entry| entry.entry_type() == EntryType::EntryCmd,
        |mut entry| Some(mem::take(&mut entry.data)),
    )
}

#[inline]
fn remove_empty_entry(entries: &mut Vec<Entry>) {
    drain_filter::<Entry, ()>(entries, |entry| entry.data.is_empty(), |_| None);
}

fn collect_changes<C: FnMut(EndpointChange)>(
    old_voters: HashSet<NodeID>,
    new_voters: HashSet<NodeID>,
    changes: Vec<ConfChange>,
    mut collect: C,
) {
    let to_removes: HashSet<_> = old_voters.difference(&new_voters).collect();
    let to_adds: HashSet<_> = new_voters.difference(&old_voters).collect();
    crate::debug!(
        "confstate changes:\nold voters: {:?}\nnew voters: {:?}\nto add: {:?},\nto remove: {:?}",
        old_voters,
        new_voters,
        to_adds,
        to_removes
    );

    for change in changes {
        let change = change.try_into();
        if let Err(_e) = change {
            continue;
        }
        let change: EndpointChange = change.unwrap();

        match change.get_changed() {
            RefChanged::AddNode(endpoint) => {
                if !to_adds.contains(&&endpoint.id) {
                    continue;
                }
            }
            RefChanged::RemoveNode(_) => {
                continue;
            }
        }
        collect(change);
    }

    for &node_id in to_removes {
        collect(EndpointChange::remove(node_id));
    }
}
