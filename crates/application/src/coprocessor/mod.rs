pub mod listener;
pub mod builder;
pub mod delegation;

pub(crate) mod driver;
mod executor;
pub(crate) mod read_index_ctx;

use std::{io::Result, sync::Arc};

use common::protos::raft_log_proto::Snapshot;
use consensus::{
    prelude::{SoftState},
    raft_node::SnapshotStatus
};

use crate::{
    mailbox::topo::PeerID, protos::raft_log_proto::Entry, RaftResult,
};

use self::{listener::{Listeners, RaftContext}, read_index_ctx::ReadContext};

/// Soft state changed reason, often used
/// to help to debug or do some tracing works.
#[derive(Debug, Clone, Copy)]
pub enum ChangeReason {
    MaybeSplit,
    Election,
    Tick,
    RecvHeartbeat,
    RecvAppend,
    RecvVote,
}

impl ChangeReason {
    #[inline]
    pub fn describe(&self) -> &str {
        match self {
            ChangeReason::MaybeSplit => "maybe I'm split when connecting to other peers",
            ChangeReason::Election => "after I initiated an election",
            ChangeReason::RecvHeartbeat => "after I received a heartbeat",
            ChangeReason::RecvVote => "after I received a vote",
            ChangeReason::Tick => "after ticked",
            ChangeReason::RecvAppend => "after I received an append",
        }
    }
}

#[crate::async_trait]
pub trait RaftCoprocessor: Sync {

    /// Start the coprocessor at runtime.
    #[inline] async fn start(&self) -> RaftResult<()> { 
        // noop
        Ok(())
    }

    /// Stop the coprocessor at runtime, maybe exists some 
    /// resources should be released.
    #[inline] async fn stop(&self) -> RaftResult<()> { 
        // noop
        Ok(())
    }
    
    #[inline]
    fn before_persist_log_entry(&self, _group: u32, _entries: &mut [Entry]) {
        /* ignore */
    }

    ///
    #[inline]
    fn after_persist_log_entry(&self, _group: u32, _result: &RaftResult<usize>) {
        /* ignore */
    }

    /// Handle commit propose data with listeners. Both available for
    /// `Leader` and `Follower`.
    async fn apply_log_entries(
        &self,
        ctx: &RaftContext,
        entries: &Vec<Entry>,
        listeners: Arc<Listeners>,
    ) -> i64;

    /// Handle committed commands with listeners. Both available for
    /// `Leader` and `Follower`.
    async fn apply_cmds(
        &self,
        ctx: &RaftContext,
        cmds: &Vec<Vec<u8>>,
        listeners: Arc<Listeners>,
    );

    /// Coprocessor allow to enhance the read_ctx before do real read
    /// work, for example add an unique ID for this read.
    async fn before_read_index(
        &self,
        read_ctx: &mut ReadContext
    ) -> RaftResult<()>;

    /// When raft generate some read state or response of "forward read"(from follower)
    /// all these result will be collected to `read_ctx` and passed in. Coprocessor
    /// should help to handle these stuffs.
    async fn advance_read(
        &self, 
        raft_ctx: &RaftContext,
        read_ctx: &mut ReadContext,
        listeners: Arc<Listeners>,
    ) -> RaftResult<()>;

    fn handle_soft_state_change(
        &self,
        peer_id: PeerID,
        prev_ss: &SoftState,
        current_ss: &SoftState,
        listeners: Arc<Listeners>,
        reason: ChangeReason,
    );

    /// **Snapshot Step 1**: Leader prepare send snapshot to follower, before that,
    /// listeners could give more informations of snapshot
    /// to target follower, even if backups of snapshot is
    /// small enough, then backups data could be set to snapshot's
    /// data.
    async fn before_send_snapshot(
        &self,
        ctx: &RaftContext,
        snapshot: &mut Snapshot,
        listeners: Arc<Listeners>,
    ) -> Result<()>;

    /// **Snapshot Step 3**: `Leader` start to send backups of snapshot
    /// after handshake with `metainfo`s in snapshot's data.
    async fn sending_backups(
        &self,
        ctx: &RaftContext,
        snapshot: Snapshot,
        listeners: Arc<Listeners>,
    ) -> Result<()>;

    /// **Snapshot Step 2**: Leader and follower will communicate with session
    /// before really send snap items
    async fn before_apply_snapshot(
        &self,
        ctx: &RaftContext,
        snapshot: &mut Snapshot,
        listeners: Arc<Listeners>,
    ) -> Result<()>;

    /// **Snapshot Step 4**: `Follower` attempt to receiving 
    /// backups from leader, ensure the leader start to send
    /// backups at step 3.
    async fn receiving_backups(
        &self,
        snapshot: &Snapshot,
        listeners: Arc<Listeners>,
    ) -> Result<()>;

    /// **Snapshot Step 5**: After `follower` applied the backups of snapshot
    /// to local.
    async fn after_applied_backups(
        &self,
        snapshot: &Snapshot,
        status: SnapshotStatus,
        listeners: Arc<Listeners>,
    );
}
