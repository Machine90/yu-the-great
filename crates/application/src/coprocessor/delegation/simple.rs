use std::io::Result;
use std::sync::Arc;

use crate::coprocessor::{
    listener::{Listener, Listeners, RaftContext},
    read_index_ctx::{ReadContext, Interested, EvaluateRead, MaybeReady},
    ChangeReason, RaftCoprocessor,
};
use crate::protos::raft_log_proto::{Entry, Snapshot};
use crate::{warn, PeerID, RaftResult};
use components::{torrent::runtime};
use common::protocol::{response::Response, read_state::ReadState};
use consensus::{
    prelude::{SoftState},
    raft_node::SnapshotStatus, 
};

/// A simple stateless coprocessor for raft.
#[derive(Debug, Clone, Copy)]
pub struct SimpleCoprocessor;

impl SimpleCoprocessor {
    pub(super) async fn read_states(
        &self,
        ctx: &RaftContext,
        read_states: &mut Vec<ReadState>,
        listeners: Arc<Listeners>,
    ) {
        // TODO: to support handle read_states in batch mode, pass
        // all read_states to reader and give another handle method, e.g.
        // handle_read_batch(read_states);
        for listener in listeners.iter() {
            match listener.value() {
                Listener::Proposal(reader) => {
                    if !listener.should_execute(ctx).await {
                        continue;
                    }
                    // then overwrite the latest result with readed result if exists some handler.
                    for rs in read_states.iter_mut() {
                        reader.handle_read(ctx, rs).await;
                    }
                }
                _ => (),
            }
        }
    }
}

#[crate::async_trait]
impl RaftCoprocessor for SimpleCoprocessor {
    async fn handle_commit_log_entry(
        &self,
        ctx: &RaftContext,
        entries: &Vec<Entry>,
        listeners: Arc<Listeners>,
    ) -> i64 {
        if listeners.is_empty() {
            warn!(
                "not RaftListener set but still remained {:?} entries should be apply",
                entries.len()
            );
        }
        let mut evaluate_changes = 0;
        for listener in listeners.iter() {
            match listener.value() {
                Listener::Proposal(proposer) => {
                    if !listener.should_execute(ctx).await {
                        continue;
                    }
                    let changes = proposer.handle_write(ctx, entries).await.unwrap_or(0);
                    evaluate_changes += changes;
                }
                _ => (),
            }
        }
        evaluate_changes
    }

    async fn handle_commit_cmds(
        &self,
        ctx: &RaftContext,
        cmds: &Vec<Vec<u8>>,
        listeners: Arc<Listeners>,
    ) {
        if listeners.is_empty() {
            warn!(
                "not `AdminListener` set but still remained {:?} cmds should be apply",
                cmds.len()
            );
        }
        for listener in listeners.iter() {
            match listener.value() {
                Listener::Admin(admin) => {
                    if !listener.should_execute(ctx).await {
                        continue;
                    }
                    admin.handle_cmds(ctx, cmds).await;
                }
                _ => (),
            }
        }
    }

    #[inline]
    async fn before_read_index(&self, _: &mut ReadContext) -> RaftResult<()> {
        Ok(())
    }

    async fn advance_read(
        &self, 
        raft_ctx: &RaftContext, 
        read_ctx: &mut ReadContext, 
        listeners: Arc<Listeners>
    ) -> RaftResult<()> {
        let ready_read = read_ctx.take_ready();
        let mut ready = match ready_read {
            MaybeReady::Succeed(ready) => {
                ready
            },
            MaybeReady::Error(e) => return Err(e),
            _ => return Ok(()),
        };

        match read_ctx.get_read() {
            Interested::Forward(_) => {
                let response = ready.responses.pop();
                if let Some(response) = response {
                    read_ctx.assert(EvaluateRead::HasForward(
                        Response::ok(response))
                    );
                }
            }
            Interested::Directly(_) => {
                self.read_states(
                    &raft_ctx,
                    &mut ready.read_states,
                    listeners,
                )
                .await;
                // take exactly one read_state.
                let rs = ready.read_states.pop();
                if let Some(rs) = rs {
                    read_ctx.assert(EvaluateRead::HasReaded(
                        Response::ok(rs))
                    );
                }
            }
            _ => (),
        };
        Ok(())
    }

    fn handle_soft_state_change(
        &self,
        peer_id: PeerID,
        prev_ss: &SoftState,
        current_ss: &SoftState,
        listeners: Arc<Listeners>,
        reason: ChangeReason,
    ) {
        for listener in listeners.iter() {
            match listener.value() {
                Listener::Proposal(change_aware) => {
                    change_aware.on_soft_state_change(
                        peer_id,
                        prev_ss.clone(),
                        current_ss.clone(),
                        reason,
                    );
                }
                _ => (),
            }
        }
    }

    async fn before_send_snapshot(
        &self,
        ctx: &RaftContext,
        snapshot: &mut Snapshot,
        listeners: Arc<Listeners>,
    ) -> Result<()> {
        for listener in listeners.iter() {
            match listener.value() {
                Listener::Snapshot(transfer) => {
                    if !listener.should_execute(ctx).await {
                        continue;
                    }
                    transfer.prepare_send_snapshot(ctx, snapshot).await?;
                }
                _ => (),
            }
        }
        Ok(())
    }

    #[allow(unused_variables)]
    async fn sending_backups(
        &self,
        ctx: &RaftContext, // TODO: keep this arg
        mut snapshot: Snapshot,
        listeners: Arc<Listeners>,
    ) -> Result<()> {
        for listener in listeners.iter() {
            match listener.value() {
                Listener::Snapshot(transfer) => {
                    transfer.sending_backups(&mut snapshot).await?;
                }
                _ => (),
            }
        }
        Ok(())
    }

    #[allow(unused_variables)]
    async fn before_apply_snapshot(
        &self,
        ctx: &RaftContext,
        snapshot: &mut Snapshot,
        listeners: Arc<Listeners>,
    ) -> Result<()> {
        for listener in listeners.iter() {
            match listener.value() {
                Listener::Snapshot(transfer) => {
                    let _ = transfer.prepare_recv_backups(snapshot).await?;
                }
                _ => (),
            }
        }
        Ok(())
    }

    async fn receiving_backups(
        &self,
        snapshot: &Snapshot,
        listeners: Arc<Listeners>,
    ) -> Result<()> {
        let mut tasks = vec![];
        for listener in listeners.iter() {
            match listener.value() {
                Listener::Snapshot(transfer) => {
                    if transfer.conf().recv_backups_parallel {
                        let transfer = transfer.clone();
                        let snap = snapshot.clone();
                        tasks.push(runtime::spawn(async move {
                            transfer.receiving_backups(snap).await
                        }));
                    } else {
                        transfer.receiving_backups(snapshot.clone()).await?;
                    }
                }
                _ => (),
            }
        }
        for task in tasks {
            task.await??;
        }
        Ok(())
    }

    async fn after_applied_backups(
        &self,
        _snapshot: &Snapshot,
        status: SnapshotStatus,
        _listeners: Arc<Listeners>,
    ) {
        crate::debug!(
            "[Snapshot Step 5] after applied backups in status: {:?}",
            status
        );
    }
}
