use common::{errors::application::Yusult, protocol::read_state::ReadState};
use super::model::{indexer::{Unique, WriteBatch}, Located};

/// The multi-raft trait require to implement route and
/// read/write actions of specific "oplog" (which implement
/// Unique trait).
#[async_trait::async_trait]
pub trait MultiRaftApi {
    /// Propose an unique "oplog" to multi-raft cluster.
    /// This oplog must be known it's index, the index
    /// will be used to find which group to propose.
    /// The committed index will be return if success.
    async fn propose<L: Unique + Send>(&self, oplog: L) -> Yusult<Located<u64>>;

    async fn propose_batch<E: Unique + Send>(&self, _write_batch: WriteBatch<E>) {
        unimplemented!("not support function propose_batch")
    }

    /// Read from multi-raft with `read_context`, the unique index
    /// of read_context must be known, then multi-raft will
    /// route this read to correct group (if exists).
    /// the binary content (after readed) will be returned.
    async fn read_index<I: Unique + Send>(&self, read_context: I) -> Yusult<Located<ReadState>>;
}
