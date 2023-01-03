use crate::{coprocessor::ChangeReason, PeerID};
use common::protocol::read_state::ReadState;
use consensus::raft::SoftState;
use std::io::Result;

use super::{Acl, RaftContext};

#[allow(unused_variables)]
#[crate::async_trait]
pub trait RaftListener: Acl + Sync {
    /// Perform write behavior in raft, when propose and commit some entries to raft,
    /// all these entries will be passed in. Suggest return changed bytes as result.
    async fn handle_write(&self, ctx: &RaftContext, data: &[u8]) -> Result<i64>;

    /// Perform read behavior in raft, when `read_index` request received from `client`
    /// (local or remote), the raft coprocessor will call this interface, and pass in
    /// `read_states` with some request context.
    async fn handle_read(&self, ctx: &RaftContext, read_states: &mut ReadState);

    /// Once the raft soft state has been changed, the previous state and current state
    /// will be passed in this interface, also the change reason will be passed in.
    /// For instance, when a `follower` init an election and became leader later, it'll
    /// publish a changed event to this interface, previous raft state is Follower,
    /// but now is Leader, reason is `Election`.
    #[allow(unused)]
    fn on_soft_state_change(
        &self,
        peer_id: PeerID,
        prev_state: SoftState,
        current_state: SoftState,
        reason: ChangeReason,
    ) {
        // could be customized
    }
}
