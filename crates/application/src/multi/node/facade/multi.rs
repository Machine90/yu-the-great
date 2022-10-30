use crate::{
    multi::node::Node,
    peer::{
        facade::AbstractPeer,
        process::proposal::{self},
    },
    ConsensusError,
};
use common::{
    errors::application::{YuError, Yusult},
    protocol::read_state::ReadState,
};
use components::{
    bincode::serialize,
    mailbox::multi::{
        api::MultiRaftApi,
        model::{
            indexer::{Unique, WriteBatch},
            Located, Location,
        },
    },
    storage::group_storage::GroupStorage,
};
use std::io::{Error, ErrorKind};

#[crate::async_trait]
impl<S: GroupStorage> MultiRaftApi for Node<S> {
    async fn propose<L: Unique + Send>(&self, oplog: L) -> Yusult<Located<u64>> {
        let group = self
            .node_manager
            .partitions()
            .current()
            .find(oplog.get_index())
            .ok_or(ConsensusError::Other(
                "should not propose to this node".into(),
            ))?
            .resident;

        let peer = self.find_peer(group)?;
        let content = serialize(&oplog).unwrap();

        let builder = Location::new((group, self.id));
        let proposal = peer.propose_async(content).await?;

        match proposal {
            proposal::Proposal::Commit(index) => Ok(builder.build(index)),
            proposal::Proposal::Pending => Err(YuError::ConsensusError(ConsensusError::Pending)),
            proposal::Proposal::Timeout => Err(YuError::IoError(Error::new(
                ErrorKind::TimedOut,
                format!(
                    "proposal timeout, total elapsed for {}ms",
                    builder.elapsed()
                ),
            ))),
        }
    }

    async fn propose_batch<E: Unique + Send>(&self, _write_batch: WriteBatch<E>) {
        todo!()
    }

    async fn read_index<I: Unique + Send>(&self, read_context: I) -> Yusult<Located<ReadState>> {
        let group = self
            .node_manager
            .partitions()
            .current()
            .find(read_context.get_index())
            .ok_or(ConsensusError::Other(
                "should not propose to this node".into(),
            ))?
            .resident;

        let peer = self.find_peer(group)?;
        let read_request = serialize(&read_context).unwrap();

        let located = Location::new((group, self.id));
        let rs = peer.read_async(read_request).await?;
        Ok(located.build(rs))
    }
}
