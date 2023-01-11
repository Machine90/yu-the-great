use crate::peer::config::NodeConfig;
use crate::protos::raft_log_proto::Entry;
use crate::tokio::{time::timeout, task::JoinHandle};
use crate::{torrent::runtime, PeerID, RaftMsg, RaftResult};
use common::protocol::{GroupID, NodeID};
use common::protos::raft_log_proto::{Snapshot};
use common::vendor::prelude::{DashMap};
use components::mailbox::{PostOffice, RaftEndpoint, topo::Topo, api::GroupMailBox};
use components::monitor::{Monitor};
use components::utils::endpoint_change::{ChangeSet, Changed};
use consensus::prelude::{SoftState};
use consensus::raft_node::SnapshotStatus;
use std::collections::HashSet;
use std::io::{Result};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering, AtomicU32};
use std::time::Duration;

use super::read_index_ctx::ReadContext;
use super::{ChangeReason};
use super::{
    executor::{Executor},
    listener::{Listener, Listeners, RaftContext},
};

#[derive(Debug)]
pub enum ApplySnapshot {
    // finished or failure
    Applied(SnapshotStatus),
    Applying(JoinHandle<Option<SnapshotStatus>>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AfterApplied {
    /// Should not flush last_applied
    Skip = 0,
    Persist = 1,
    Compact = 2
}

#[derive(Default)]
pub(crate) struct AppliedTracker {
    progress: DashMap<GroupID, AppliedProgress>,
}

impl AppliedTracker {

    fn try_update(&self, group: GroupID, applied: u64, conf: &NodeConfig, monitor: Option<&Monitor>) -> AfterApplied {
        self.progress
            .entry(group)
            .or_insert(AppliedProgress::default())
            .downgrade()
            .try_update(applied, conf, monitor)
    }

    #[inline]
    fn remove_group(&self, group: GroupID) {
        let _ = self.progress.remove(&group);
    }
}

#[derive(Default)]
pub(crate) struct AppliedProgress {
    last_applied: AtomicU64,
    applied_persistence_cnt: AtomicU32,
}

impl AppliedProgress {

    /// Try to update `last_applied` index to given `applied` if difference more than 
    /// `apply_persistence_index_frequency`, and return a suggestion, the suggestion 
    /// is used to help to persistence raft applied index, or clear raft logs.
    fn try_update(&self, applied: u64, conf: &NodeConfig, monitor: Option<&Monitor>) -> AfterApplied {
        let origin_applied = self.last_applied.load(Ordering::Relaxed);
        let mut suggestion = AfterApplied::Skip;
        if let Some(_monitor) = monitor {
            // TODO: do some protection
        }
        
        if applied <= origin_applied {
            return AfterApplied::Skip;
        }
        let diff = applied - origin_applied;
        
        if diff >= conf.apply_persistence_index_frequency {
            // upgrade level
            suggestion = AfterApplied::Persist;
        }
        match suggestion {
            AfterApplied::Skip => (),
            AfterApplied::Persist => {
                if let Err(_) = self.last_applied.compare_exchange(
                    origin_applied, 
                    applied, 
                    Ordering::Acquire, 
                    Ordering::Relaxed
                ) {
                    // means othe thread update applied already, just skip this update.
                    suggestion = AfterApplied::Skip;
                } else {
                    let mut should_clear = false;
                    let _ = self.applied_persistence_cnt.fetch_update(
                        Ordering::SeqCst, 
                        Ordering::SeqCst, 
                        |cnt| {
                            let next = cnt + 1;
                            if next >= conf.apply_clear_logs_frequency {
                                should_clear = true;
                                Some(0)
                            } else {
                                Some(next)
                            }
                        }
                    );
                    if should_clear {
                        // upgrade to compact raft logs.
                        suggestion = AfterApplied::Compact;
                    }
                }
            },
            _ => unreachable!()
        };
        suggestion
    }
}

/// Coprocessor is the core component of application (both multi-raft and single)
/// which used to coordinate "write" and "read" actions, and notify some changes
/// like change role, leader etc, and also monitoring some metrics in it's monitor.
pub struct CoprocessorDriver {
    pub(super) coprocessor: Executor,
    pub(super) listeners: Arc<Listeners>,
    pub(super) topo: Topo,
    pub(super) post_office: Arc<dyn PostOffice>,
    pub(crate) monitor: Option<Monitor>,
    pub(crate) endpoint: RaftEndpoint,
    pub(crate) applied_tracker: AppliedTracker,
    pub(super) conf: NodeConfig
}

impl CoprocessorDriver {

    pub async fn start(&self) -> RaftResult<()> {
        if let Some(monitor) = self.monitor.as_ref() {
            monitor.start().await;
        }
        self.coprocessor.start().await?;
        Ok(())
    }

    pub async fn stop(&self) -> RaftResult<()> {
        if let Some(moitor) = self.monitor.as_ref() {
            moitor.stop();
        }
        self.coprocessor.stop().await?;
        Ok(())
    }

    #[inline]
    pub fn monitor(&self) -> Option<&Monitor> {
        self.monitor.as_ref()
    }

    #[inline]
    pub fn topo(&self) -> &Topo {
        &self.topo
    }

    #[inline]
    pub fn node_id(&self) -> NodeID {
        self.conf.id
    }

    #[inline]
    pub fn conf(&self) -> &NodeConfig {
        &self.conf
    }

    #[inline]
    pub fn build_mailbox(&self, group: GroupID) -> Arc<dyn GroupMailBox> {
        self.post_office.build_group_mailbox(group, &self.topo)
    }

    #[inline]
    pub fn apply_voters(&self, to_group: GroupID, nodes: Vec<RaftEndpoint>) -> HashSet<PeerID> {
        self.post_office.appy_group_voters(
            to_group, 
            HashSet::default(), 
            &self.topo, 
            nodes
        )
    }

    #[inline]
    pub fn establish_connections(&self, incoming: HashSet<PeerID>) -> HashSet<PeerID> {
        self.post_office.establish_connection(incoming, &self.topo)
    }

    #[inline]
    pub fn add_listener(&self, listener: Listener) {
        self.listeners.add(listener);
    }

    #[inline]
    pub fn on_remove_group(&self, group: GroupID) {
        self.applied_tracker.remove_group(group);
    }

    ///////////////////////////////////////////////////////////////
    ///                Actions for Follower & Leader
    ///////////////////////////////////////////////////////////////

    pub fn handle_group_conf_change(&self, ctx: &RaftContext, changes: ChangeSet) -> HashSet<PeerID> {
        let RaftContext { group_id, .. } = ctx;
        let group = *group_id;
        let topo = &self.topo;
        let mut incoming = HashSet::with_capacity(changes.len());
        for change in changes.take_list() {
            match change.take_changed() {
                Changed::AddNode(node) => {
                    let node_id = node.id;
                    if topo.contained_group_node(&group, &node_id) {
                        continue;
                    }
                    topo.add_node(group, node);
                    incoming.insert((group, node_id));
                }
                Changed::RemoveNode(node_id) => {
                    crate::debug!("conf changes: remove voters {:?}", node_id);
                    topo.remove_node(&group, &node_id);
                }
            }
        }
        let maybe_existed = self.establish_connections(incoming.clone());
        if !maybe_existed.is_empty() {
            crate::debug!("these peers maybe existed: {:?}", maybe_existed);
        }
        incoming
    }

    pub async fn handle_soft_state_change(
        &self,
        peer_id: PeerID,
        prev_soft_state: &SoftState,
        soft_state: &SoftState,
        reason: ChangeReason,
    ) {
        self.coprocessor.handle_soft_state_change(
            peer_id,
            prev_soft_state,
            soft_state,
            self.listeners.clone(),
            reason,
        );
    }

    #[inline]
    pub fn before_persist_log_entry(&self, _group: u32, _entries: &mut [Entry]) {
        // ignore
    }

    #[inline]
    pub fn after_persist_log_entry(&self, _group: u32, _result: &RaftResult<usize>) {
        // ignore
    }

    /// Handle commit log_entry with coprocessors (in serialize).
    pub async fn apply_log_entry(&self, ctx: &RaftContext, data: Vec<u8>) -> RaftResult<()> {
        // handle and dispatch entry in ConfChange or Normal
        let throughput = self.coprocessor
            .apply_log_entry(&ctx, data, self.listeners.clone())
            .await?;

        if throughput == 0 { return Ok(()); }

        // if get any changes from listener, log it.
        self.monitor().map(|monitor| {
            let prober = monitor.probe();
            prober.write_node(throughput);
            if !monitor.conf.enable_group_sampling {
                return;
            }
            prober.write_group(ctx.group_id, throughput);
        });

        // TODO list:
        // 1. Compact raftlog by configured policy, rather than do it each time
        // after committed, for example do compact in schedule.
        // 2. Another optimize is, should do snapshot (make backups) before compact?
        Ok(())
    }

    /// Apply entry with [EntryCmd](crate::protos::raft_log_proto::EntryType::EntryCmd) on this node.
    pub async fn apply_command(
        &self,
        ctx: &RaftContext,
        command: Vec<u8>,
    ) -> RaftResult<()> {
        // handle and dispatch commands
        self.coprocessor
            .apply_command(ctx, command, self.listeners.clone()).await
    }

    /// After advance applied index, `applied_index` always smaller or equal to `commit_index`,
    /// if applied, means log entries before applied can be removed (but not required).
    pub async fn after_applied(
        &self, 
        ctx: &RaftContext,
    ) -> AfterApplied {
        let RaftContext { 
            applied,
            group_id,
            ..
        } = ctx;
        let group = *group_id;
        let applied = *applied;
        let conf = &self.conf;
        let monitor = self.monitor();
        self.applied_tracker.try_update(group, applied, conf, monitor)
    }

    /// When leader receive raw read_index ctx directly or from
    /// forward, before raft `step` it, this method will be called.
    /// `read_ctx` could be modified in coprocessor, for example give
    /// a request id to ctx.
    #[inline]
    pub async fn before_read_index(&self, read_ctx: &mut ReadContext) -> RaftResult<()> {
        self.coprocessor.before_read_index(read_ctx).await
    }

    #[inline]
    pub async fn advance_read(
        &self, 
        raft_ctx: &RaftContext,
        read_ctx: &mut ReadContext
    ) -> RaftResult<()> {
        if !read_ctx.ready.is_ready() {
            return Ok(());
        }
        self.coprocessor.advance_read(raft_ctx, read_ctx, self.listeners.clone()).await
    }

    ///////////////////////////////////////////////////////////////
    ///                Actions for Leader
    ///////////////////////////////////////////////////////////////

    /// Before leader send snapshot type msg to follower.
    pub async fn before_send_snapshot(
        &self,
        ctx: &RaftContext,
        snapshot: &mut Snapshot,
    ) -> Result<()> {
        // do handshake with follower before send each snapshot.
        // todo load backups info from listener, and set to snapshot ctx. told
        // remote site what items I prepare to send.
        crate::debug!(
            "[Snapshot Step 1] prepare send snapshot: {:?}",
            snapshot.get_metadata()
        );
        self.coprocessor
            .before_send_snapshot(ctx, snapshot, self.listeners.clone())
            .await
    }

    pub async fn do_send_snapshot(
        &self,
        ctx: &RaftContext,
        snapshot: Snapshot,
    ) -> Result<()> {
        crate::debug!(
            "[Snapshot Step 3] sending snapshot: {:?}",
            snapshot.get_metadata()
        );
        self.coprocessor
            .sending_backups(ctx, snapshot, self.listeners.clone())
            .await
    }

    pub fn after_send_snapshots(&self, _ctx: &RaftContext, snapshots: Vec<RaftMsg>) {
        crate::debug!(
            "[Snapshot Step 6] after send {:?} snapshot",
            snapshots.len()
        );
    }

    ///////////////////////////////////////////////////////////////
    ///                Actions for Follower
    ///////////////////////////////////////////////////////////////

    pub async fn before_apply_snapshot(
        &self,
        ctx: &RaftContext,
        snapshot: &mut Snapshot,
    ) -> std::io::Result<()> {
        crate::debug!(
            "[Snapshot Step 2] prepare apply snapshot: {:?}",
            snapshot.get_metadata()
        );
        self.coprocessor
            .before_apply_snapshot(ctx, snapshot, self.listeners.clone())
            .await?;
        Ok(())
    }

    /// Do applying the backups in a standalone task, then return result in-place if 
    /// finish syncing in timeout (if backups small enough), otherwise return the transferring 
    /// task and handle it in future in process.
    pub async fn do_apply_snapshot(
        &self,
        _ctx: &RaftContext,
        snapshot: &Snapshot,
        wait_timeout: Duration,
    ) -> ApplySnapshot {
        crate::debug!(
            "[Snapshot Step 4] applying snapshot: {:?}",
            snapshot.get_metadata()
        );

        let (notifier, recv) = crate::tokio::sync::oneshot::channel();

        let executor = self.coprocessor.clone();
        let listeners = self.listeners.clone();

        let snap = snapshot.clone();

        // first, handle receiving task in an async worker, then executor keep receiving backups.
        let transferring = runtime::spawn(async move {
            let try_apply = executor.receiving_backups(&snap, listeners.clone()).await;
            // determine if snapshot transferring success
            let status = if let Err(e) = try_apply {
                crate::error!(
                    "attempt to receive backups of {:?} and apply it, but failed, see: {:?}",
                    snap, e
                );
                SnapshotStatus::Failure
            } else {
                SnapshotStatus::Finish
            };

            executor.after_applied_backups(&snap, status, listeners).await;

            // try to notify outside if finish transferring.
            // if got err, means timeout, then just handle it by self.
            notifier.send(status).err()
        });

        let apply_in_timeout = timeout(wait_timeout, recv);
        let apply_in_timeout = apply_in_timeout.await;
        if let Err(_) = apply_in_timeout {
            // so.. return the inflight task if exceed transfer timeout. maybe success in future.
            return ApplySnapshot::Applying(transferring);
        }
        // blocking for accept snapshot result in given timeout.
        match apply_in_timeout.unwrap() {
            Ok(status) => ApplySnapshot::Applied(status),
            Err(e) => {
                crate::error!(
                    "failed to apply snapshot in {:?}ms, see: {:?}",
                    wait_timeout.as_millis(),
                    e
                );
                ApplySnapshot::Applied(SnapshotStatus::Failure)
            }
        }
    }
}

#[cfg(test)] mod tests {
    use std::sync::Arc;
    use crate::peer::config::NodeConfig;
    use super::AppliedTracker;

    #[test] fn multi_applier() {
        let applier = Arc::new(AppliedTracker::default());
        let mut ts = vec![];
        
        const PERSIST_FREQ: u64 = 10;
        const COMPACT_FREQ: u32 = 5;
        const APPLIED_TO: u64 = 1000;

        for i in 0..20 {
            let id = i % 7;
            let appl = applier.clone();
            
            ts.push(std::thread::spawn(move || {
                let conf = NodeConfig { 
                    apply_persistence_index_frequency: PERSIST_FREQ,
                    apply_clear_logs_frequency: COMPACT_FREQ, 
                    ..Default::default() 
                };
                let mut compact = 0;
                let mut persistent_applied = 0;
                for applied in 1..=APPLIED_TO {
                    match appl.try_update(id, applied, &conf, None) {
                        super::AfterApplied::Compact => {
                            persistent_applied += 1;
                            compact += 1;
                        },
                        super::AfterApplied::Persist => {
                            persistent_applied += 1;
                        }
                        _ => ()
                    }
                }
                (id, persistent_applied, compact)
            }));
        }

        let mut records = vec![(0, 0); 7];
        for t in ts {
            if let Ok((id, persist, compact)) = t.join() {
                records[id as usize].0 += persist;
                records[id as usize].1 += compact;
            }
        }
        for (persist, compact) in records {
            assert_eq!(persist, APPLIED_TO / PERSIST_FREQ);
            assert_eq!(compact, APPLIED_TO / (PERSIST_FREQ * COMPACT_FREQ as u64))
        }
    }
}