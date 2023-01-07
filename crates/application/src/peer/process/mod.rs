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
use crate::coprocessor::driver::AfterApplied;
use crate::protos::prelude::prost::Message;
use crate::torrent::runtime;
use crate::{
    coprocessor::listener::RaftContext,
    error,
    storage::{ReadStorage, WriteStorage},
    tokio::task::JoinHandle,
    RaftMsg, RaftResult,
};
use crate::{
    coprocessor::{driver::ApplySnapshot, ChangeReason},
    protos::{
        prelude::BatchConfChange,
        raft_log_proto::{Entry, EntryType, HardState},
    },
    NodeID,
};
use common::protos::raft_conf_proto::ConfChange;
use common::protos::raft_log_proto::Snapshot;
use components::mailbox::api::MailBox;
use components::utils::endpoint_change::{ChangeSet, EndpointChange, RefChanged};
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
            .handle_soft_state_change(peer_id, pss, ss, reason)
            .await;
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
    ) -> RaftResult<ApplySnapshot> {
        // first, try apply this snapshot request to store, maybe reject if
        // snap out of date. if approved, means this peer's log entries is expired,
        // then just feel free to clear them.
        self.write_store
            .apply_snapshot(snapshot.get_metadata().clone())?;

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
            return Ok(ApplySnapshot::Applied(SnapshotStatus::Failure));
        }

        // step2: then do really accept snapshot items, for example blocking incoming
        // stream in a standalone thread, and get apply state from it, maybe pending.
        let apply_state = self
            .coprocessor_driver
            .do_apply_snapshot(
                ctx,
                &snapshot,
                self.conf().max_wait_backup_tranfer_duration(),
            )
            .await;

        match apply_state {
            ApplySnapshot::Applied(status) if status == SnapshotStatus::Finish => {
                // if finished receive backups, then apply to raft log.
                crate::debug!(
                    "finish transferring backup in-place, result is: {:?}",
                    status
                );
            }
            _ => (),
        }
        Ok(apply_state)
    }

    /// Apply entries to 
    async fn apply_commit_entries(
        &self,
        raft: &mut WLockRaft<'_>,
        entries: Vec<Entry>,
    ) -> (bool, Option<u64>) {
        if entries.is_empty() {
            return (true, None);
        }

        let cp_driver = self.coprocessor_driver.as_ref();
        let group_id = self.group_id;
        let ctx = RaftContext::from_status(group_id, raft.status());

        let mut last_applied = None;
        let mut complete = true;
        for mut entry in entries {
            let index = entry.index;
            let content = mem::take(&mut entry.data);

            let applied_result = match entry.entry_type() {
                EntryType::EntryNormal => {
                    if !content.is_empty() {
                        cp_driver.apply_log_entry(&ctx, content).await
                    } else {
                        Ok(())
                    }
                    // otherwise skip the empty entry.
                }
                EntryType::EntryConfChange => {
                    // TODO: return apply result and handle error case.
                    self._apply_conf_change(&ctx, raft, content).await
                }
                EntryType::EntryCmd => {
                    cp_driver.apply_command(&ctx, content).await
                }
            };
            if let Err(e) = applied_result {
                // How to handle if apply log failure, for example disk unavailable.
                // Some discussions about this problem:
                // 1. https://groups.google.com/g/raft-dev/c/fbwv8-qMFYE?pli=1
                // 2. https://stackoverflow.com/questions/54938499/how-to-handle-the-saving-failure-after-raft-committed
                // 3. https://github.com/hashicorp/raft/issues/307
                // The conclusion is that panic if apply log failed, then shutdown the node.
                // Before shutdown, we should record last applied.
                crate::error!("failed to apply entry-{index} to listeners, see: {:?}", e);
                complete = false;
                break;
            }
            last_applied = Some(index);
        }
        (complete, last_applied)
    }

    async fn _apply_conf_change(
        &self,
        ctx: &RaftContext,
        raft: &mut WLockRaft<'_>,
        content: Vec<u8>,
    ) -> RaftResult<()> {
        let conf_channge = if content.is_empty() {
            // maybe generate an empty entry with type `EntryConfChange`
            // when `auto_leave` in `commit_apply` after some changes complete.
            BatchConfChange::default()
        } else {
            BatchConfChange::decode(&content[..]).unwrap_or_default()
        };

        let (ori, new) = raft.apply_conf_change(&conf_channge)?;
        let mut change_list = ChangeSet::new();
        collect_changes(
            ori.all_voters(),
            new.all_voters(),
            conf_channge.changes,
            |change| {
                change_list.insert(change);
            },
        );

        if change_list.is_empty() {
            return Ok(());
        }

        let is_leader = ctx.role.is_leader();

        let should_connect = self
            .coprocessor_driver
            .handle_group_conf_change(ctx, change_list);

        self.write_store.set_conf_state(new)?;

        if is_leader && !should_connect.is_empty() {
            let group_info = self.group_info();
            if group_info.is_none() {
                return Ok(());
            }

            let group_info = group_info.unwrap();
            if should_connect.len() == 1 {
                // most situation is to add 1 node in a propose.
                // send this group info without any clone cost.
                let (_, node_id) = should_connect.iter().next().unwrap();
                self.mailbox.sync_with(*node_id, group_info).await;
            } else {
                for (_, node_id) in should_connect {
                    self.mailbox.sync_with(node_id, group_info.clone()).await;
                }
            }
        }
        Ok(())
    }

    /// Finish process and try updating last apply index to given value. 
    /// Then release the raft lock.
    #[inline]
    async fn _finish_and_apply_to(
        &self, 
        mut raft: WLockRaft<'_>, 
        applied: Option<u64>, 
        has_complete: bool,
        fetch_context: bool
    ) -> Option<RaftContext> {
        let ctx = self._advance_apply_to(&mut raft, applied, has_complete).await;
        if fetch_context {
            ctx.or(Some(RaftContext::from_status(self.group_id, raft.status())))
        } else {
            None
        }
    }

    async fn _advance_apply_to(&self, raft: &mut WLockRaft<'_>, applied: Option<u64>, has_complete: bool) -> Option<RaftContext> {
        if applied.is_none() {
            return None;
        }
        // first, update applied value.
        let (old, update) = if let Some(update) = applied {
            let old = raft.advance_apply_to(update);
            (old, update)
        } else {
            raft.advance_apply()
        };
        // then, fetch latest status of raft.
        if old < update {
            // maybe equal
            let ctx: RaftContext = RaftContext::from_status(self.group_id, raft.status());
            let suggestion = self.coprocessor_driver.after_applied(&ctx).await;
            if suggestion >= AfterApplied::Persist || !has_complete {
                self._persist_last_applied(update, has_complete);
            }
            if suggestion == AfterApplied::Compact {
                // do compact raft log
                self._compact_raft_log(update, true).await;
            }
            Some(ctx)
        } else {
            None
        }
    }

    fn _persist_last_applied(&self, index: u64, has_complete: bool) {
        // persist last applied
        if let Err(e) = self.write_store.update_applied(index) {
            crate::error!("failed to write applied {index}, see: {:?}", e);
        }
        
        // panic if not has complete and should not ignore failure
        if !has_complete && !self.conf().ignore_apply_failure {
            crate::error!("apply log entry failure, decide to abort");
            // TODO: shutdown more graceful.
            panic!("apply log entry failure");
        }
    }
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
