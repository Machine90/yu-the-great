use super::{Acl, RaftContext};
use std::io;

#[crate::async_trait]
pub trait AdminListener: Acl + Sync {
    /// Handle commands as admin, the command which has entry type `EntryType::Cmd`.
    async fn handle_command(
        &self,
        ctx: &RaftContext,
        command: &Vec<u8>
    ) -> io::Result<()>;
}
