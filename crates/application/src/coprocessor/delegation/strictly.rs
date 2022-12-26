use std::io::Result;
use std::sync::Arc;

use super::{encode_unique_read, decode_unique_read};
use super::simple::SimpleCoprocessor;
use crate::coprocessor::listener::{Listeners, RaftContext};
use crate::coprocessor::{read_index_ctx::ReadContext, RaftCoprocessor, ChangeReason};
use crate::protos::raft_log_proto::Entry;
use crate::tokio::sync::RwLock;
use crate::{PeerID, NodeID};
use crate::{RaftResult};
use common::protos::raft_log_proto::Snapshot;
use components::vendor::prelude::id_gen::{Snowflake, IDGenerator};
use components::vendor::prelude::id_gen::Policy;
use consensus::prelude::SoftState;
use consensus::raft_node::SnapshotStatus;
use crate::coprocessor::read_index_ctx::{MaybeReady};

pub struct LinearCoprocessor {
    id_gen: Arc<RwLock<Snowflake>>,
    based: SimpleCoprocessor,
}

impl LinearCoprocessor {
    /// Linear raft coprocessor. Used to help to handle
    /// raft procedure in right way, for example handle committed
    /// entries and read if there has any listeners.
    ///
    /// ### Params
    /// * node: use node_id to create a request_id generator, this generator can
    /// generate amount of request id in each milli-seconds.
    /// In fact, request id may not save too long, request ctx will be saved in
    /// [ReadOnly](consensus::raft::read_only::normal::NormalRead), and took out
    /// after leader heartbeat success.
    pub fn new(node: NodeID) -> Self {
        let gen = Snowflake::from_timestamp(Policy::N20(node as u32, true), 1660369920287).unwrap();
        Self {
            id_gen: Arc::new(RwLock::new(gen)),
            based: SimpleCoprocessor,
        }
    }

    #[inline]
    pub async fn req_id(&self) -> u64 {
        self.id_gen.write().await.next_id()
    }
}

#[crate::async_trait]
impl RaftCoprocessor for LinearCoprocessor {
    #[inline] async fn start(&self) -> RaftResult<()> { 
        crate::info!("[Coprocessor] start strictly coprocessor");
        Ok(())
    }

    #[inline(always)]
    async fn apply_log_entries(
        &self,
        ctx: &RaftContext,
        entries: &Vec<Entry>,
        listeners: Arc<Listeners>,
    ) -> i64 {
        self.based
            .apply_log_entries(ctx, entries, listeners)
            .await
    }

    #[inline(always)]
    async fn apply_cmds(
        &self,
        ctx: &RaftContext,
        cmds: &Vec<Vec<u8>>,
        listeners: Arc<Listeners>,
    ) {
        self.based
            .apply_cmds(ctx, cmds, listeners)
            .await
    }

    async fn before_read_index(&self, read_ctx: &mut ReadContext) -> RaftResult<()> {
        let read_id = self.req_id().await;
        // it's necessarily to encode with unique id here, because
        // the same read_ctx would not insert into `read_only` repeated
        encode_unique_read(read_id, read_ctx);
        Ok(())
    }

    async fn advance_read(
        &self, 
        raft_ctx: &RaftContext, 
        read_ctx: &mut ReadContext, 
        listeners: Arc<Listeners>
    ) -> RaftResult<()> {
        let ready_read = read_ctx.take_ready();
        let mut ready_read = match ready_read {
            MaybeReady::Succeed(ready_read) => ready_read,
            MaybeReady::Error(e) => return Err(e), 
            _ => return Ok(())
        };
        for rs in ready_read.read_states.iter_mut() {
            rs.request_ctx = decode_unique_read(&rs.request_ctx[..]).to_vec();
        }
        read_ctx.with_ready(ready_read);
        self.based.advance_read(raft_ctx, read_ctx, listeners).await
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
