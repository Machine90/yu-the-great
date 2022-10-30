use super::{Acl, RaftContext};

#[crate::async_trait]
pub trait AdminListener: Acl + Sync {
    /// Handle commands as admin, the command which has entry type `EntryType::Cmd`.
    async fn handle_cmds(
        &self,
        ctx: &RaftContext,
        cmds: &Vec<Vec<u8>>
    );
}
