use std::io::Result;
use std::sync::Arc;
use std::time::Duration;

use super::encode_unique_read;
use super::simple::SimpleCoprocessor;
use crate::coprocessor::delegation::decode_unique_read;
use crate::coprocessor::listener::{Listeners, RaftContext, Listener};
use crate::coprocessor::{read_index_ctx::ReadContext, ChangeReason, RaftCoprocessor};
use crate::peer::process::read::ReadyRead;
use crate::protos::raft_log_proto::Entry;
use crate::PeerID;
use crate::{RaftMsg, RaftResult, debug, ConsensusError};
use common::protocol::NodeID;
use common::protocol::read_state::ReadState;
use common::protos::raft_log_proto::Snapshot;
use common::protocol::response::Response;
use components::utils::pending::{Pending};
use consensus::prelude::SoftState;
use consensus::raft_node::SnapshotStatus;

use crate::coprocessor::read_index_ctx::{Interested, EvaluateRead, MaybeReady};

pub struct Conf {
    max_receivers_per_read: usize,
    read_timeout_millis: u64,
}

impl Conf {

    #[inline]
    pub fn get_read_timeout(&self) -> Duration {
        Duration::from_millis(self.read_timeout_millis)
    }
}

impl Default for Conf {
    fn default() -> Self {
        Self { 
            max_receivers_per_read: 4096, 
            read_timeout_millis: 1000 
        }
    }
}

pub struct BatchCoprocessor {
    conf: Conf,
    based: SimpleCoprocessor,
    pending_request: Pending<Vec<u8>, Response<ReadState>>,
    pending_forward: Pending<Vec<u8>, Response<RaftMsg>>,
    node: NodeID
}

impl BatchCoprocessor {
    /// Delegation raft coprocessor. Used to help to handle
    /// raft procedure in right way, for example handle committed
    /// propose and read if there has any listeners.
    ///
    /// ### Params
    /// * node: use node_id to create a request_id generator, this generator can
    /// generate amount of request id in each milli-seconds.
    /// In fact, request id may not save too long, request ctx will be saved in
    /// [ReadOnly](consensus::raft::read_only::normal::NormalRead), and took out
    /// after leader heartbeat success.
    pub fn new(node: NodeID) -> Self {
        Self {
            conf: Conf::default(),
            based: SimpleCoprocessor,
            pending_request: Pending::default(),
            pending_forward: Pending::default(),
            node
        }
    }

    async fn advance_failure_read(
        &self, 
        topic: &Interested,
        err: ConsensusError
    ) {
        match topic {
            Interested::Forward(topic) => {
                if let Some(notifier) = self
                    .pending_forward
                    .take_notifier(topic) 
                {
                    let _ = notifier.send(err.into());
                }
            },
            Interested::Directly(topic) => {
                if let Some(notifier) = self
                    .pending_request
                    .take_notifier(topic) 
                {
                    let _ = notifier.send(err.into());
                }
            },
            _ => (),
        }
    }

    async fn advance_read_ready(
        &self, 
        mut ready: ReadyRead,
        raft_ctx: &RaftContext,
        listeners: Arc<Listeners>,
    ) {
        if ready.is_empty() { return; }
        for read_resp in ready.responses {
            let read_ctx = &read_resp.entries[0].data;
            if let Some(notifier) = self
                .pending_forward
                .take_notifier(read_ctx) 
            {
                let try_notify = notifier.send(
                    Response::ok(read_resp)
                );
                if let Err(e) = try_notify {
                    crate::warn!("failed to notify `read_response`: {:?}", e);
                }
            }
        }

        for rs in ready.read_states.iter_mut() {
            let notifier = self
                .pending_request
                .take_notifier(&rs.request_ctx);
            if notifier.is_none() {
                debug!("not notifier of read_index: {:?}, ignore it", &rs.request_ctx);
                continue;
            }
            rs.request_ctx = decode_unique_read(&rs.request_ctx[..]).to_vec();
            for listener in listeners.iter() {
                match listener.value() {
                    Listener::Proposal(reader) => {
                        if !listener.should_execute(&raft_ctx).await {
                            continue;
                        }
                        reader.handle_read(&raft_ctx, rs).await;
                    },
                    _ => (),
                }
            }
            let try_notify = notifier.unwrap().send(
                Response::ok(rs.clone())
            );

            if let Err(e) = try_notify {
                crate::warn!("failed to notify `read_state`: {:?}", e);
            }
        }
    }
}

#[crate::async_trait]
impl RaftCoprocessor for BatchCoprocessor {
    
    #[inline] async fn start(&self) -> RaftResult<()> { 
        crate::info!("[Coprocessor] start batch coprocessor");
        Ok(())
    }

    #[inline(always)]
    async fn handle_commit_log_entry(
        &self,
        ctx: &RaftContext,
        entries: &Vec<Entry>,
        listeners: Arc<Listeners>,
    ) -> i64 {
        self.based
            .handle_commit_log_entry(ctx, entries, listeners)
            .await
    }

    #[inline(always)]
    async fn handle_commit_cmds(
        &self,
        ctx: &RaftContext,
        cmds: &Vec<Vec<u8>>,
        listeners: Arc<Listeners>,
    ) {
        self.based
            .handle_commit_cmds(ctx, cmds, listeners)
            .await
    }

    #[inline(always)]
    async fn before_read_index(&self, read_ctx: &mut ReadContext) -> RaftResult<()> {
        let id = self.node;
        encode_unique_read(id, read_ctx);

        // read_ctx
        match read_ctx.get_read() {
            Interested::Forward(read) => {
                match self.pending_forward.sub_or_pub(
                    read.clone(), 
                    self.conf.max_receivers_per_read
                ) {
                    (recv, true) => {
                        read_ctx.assert(EvaluateRead::ShouldForward(recv));
                    },
                    (recv, false) => {
                        read_ctx.assert(EvaluateRead::HasForward(
                            recv.get(self.conf.get_read_timeout()).await?
                        ));
                    }
                };
            },
            Interested::Directly(read) => {
                match self.pending_request.sub_or_pub(
                    read.clone(), 
                    self.conf.max_receivers_per_read
                ) {
                    (recv, true) => {
                        read_ctx.assert(EvaluateRead::ShouldRead(recv));
                    },
                    (recv, false) => {
                        read_ctx.assert(EvaluateRead::HasReaded(
                            recv.get(self.conf.get_read_timeout()).await?
                        ));
                    }
                };
            },
            _ => (),
        };
        Ok(())
    }

    async fn advance_read(
        &self,
        raft_ctx: &RaftContext,
        read_ctx: &mut ReadContext,
        listeners: Arc<Listeners>,
    ) -> RaftResult<()> {
        let ready = read_ctx.take_ready();
        let ready = match ready {
            MaybeReady::Succeed(ready) => ready,
            MaybeReady::Error(err) => {
                self.advance_failure_read(read_ctx.get_read(), err).await;
                return Ok(());
            },
            _ => return Ok(())
        };

        self.advance_read_ready(
            ready, 
            &raft_ctx, 
            listeners
        ).await;
        Ok(())
    }

    #[inline(always)]
    fn handle_soft_state_change(
        &self,
        peer_id: PeerID,
        prev_ss: &SoftState,
        current_ss: &SoftState,
        listeners: Arc<Listeners>,
        reason: ChangeReason,
    ) {
        self.based
            .handle_soft_state_change(peer_id, prev_ss, current_ss, listeners, reason)
    }

    #[inline(always)]
    async fn before_send_snapshot(
        &self,
        ctx: &RaftContext,
        snapshot: &mut Snapshot,
        listeners: Arc<Listeners>,
    ) -> Result<()> {
        self.based
            .before_send_snapshot(ctx, snapshot, listeners)
            .await
    }

    #[inline(always)]
    #[allow(unused_variables)]
    async fn sending_backups(
        &self,
        ctx: &RaftContext, // TODO: keep this arg
        snapshot: Snapshot,
        listeners: Arc<Listeners>,
    ) -> Result<()> {
        self.based.sending_backups(ctx, snapshot, listeners).await
    }

    #[inline(always)]
    #[allow(unused_variables)]
    async fn before_apply_snapshot(
        &self,
        ctx: &RaftContext,
        snapshot: &mut Snapshot,
        listeners: Arc<Listeners>,
    ) -> Result<()> {
        self.based
            .before_apply_snapshot(ctx, snapshot, listeners)
            .await
    }

    #[inline(always)]
    async fn receiving_backups(
        &self,
        snapshot: &Snapshot,
        listeners: Arc<Listeners>,
    ) -> Result<()> {
        self.based.receiving_backups(snapshot, listeners).await
    }

    #[inline(always)]
    async fn after_applied_backups(
        &self,
        _snapshot: &Snapshot,
        status: SnapshotStatus,
        _listeners: Arc<Listeners>,
    ) {
        self.based
            .after_applied_backups(_snapshot, status, _listeners)
            .await
    }
}
