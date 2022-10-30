use std::hint::spin_loop;
use std::ops::Deref;
use std::sync::Arc;

use crate::peer::facade::{
    local::LocalPeer,
    mailbox::{GroupManage, MailboxService}, AbstractPeer,
};
use crate::{
    multi::node::{manager::NodeManager, Node},
    storage::group_storage::GroupStorage,
    tokio::runtime::Runtime,
    ConsensusError, GroupID, RaftResult,
};
use common::protos::raft_group_proto::GroupProto;
use components::{mailbox::api::NodeMailbox, utils::endpoint_change::ChangeSet};
use components::torrent::runtime;

impl<S: GroupStorage + Clone> Node<S> {
    fn _tick_pool(&self) -> std::io::Result<Runtime> {
        let rt = crate::tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("multi-raft-ticker")
            .build();
        Ok(rt.unwrap())
    }

    pub fn start<R, Output>(&self, runner: R) -> std::io::Result<Output>
    where
        R: FnOnce(Arc<dyn NodeMailbox>) -> Output,
    {
        let nm = self.node_manager.clone();
        let scheduler = self.scheduler.clone();
        let ticker_pool = self._tick_pool()?;

        let initializer = nm.clone();
        let signal = self.terminate.clone();

        std::thread::spawn(move || {
            ticker_pool.block_on(async move {
                if let Err(_) = initializer.start(scheduler).await {
                    signal.stop();
                    return;
                }
                while signal.is_running() {
                    spin_loop();
                }
            });
        });
        let serivce: Arc<dyn NodeMailbox> = Arc::new(MailboxService {
            manager: Arc::new(MultiMailbox { nm }),
        });
        Ok(runner(serivce))
    }
}

/// Performs multi-raft's mailbox server, could be
/// used for receiving mails from other peers in
/// different groups.
#[derive(Clone)]
pub struct MultiMailbox<S: GroupStorage> {
    nm: Arc<NodeManager<S>>,
}

impl<S: GroupStorage> Deref for MultiMailbox<S> {
    type Target = NodeManager<S>;

    fn deref(&self) -> &Self::Target {
        self.nm.as_ref()
    }
}

impl<S: GroupStorage + Clone> GroupManage for MultiMailbox<S> {
    #[inline]
    fn get_group(&self, group: GroupID) -> RaftResult<LocalPeer> {
        let peer = self.find_peer(group);
        peer.or(Err(ConsensusError::StepPeerNotFound))
    }

    fn create_group(&self, group_info: GroupProto) {
        let nm = self.nm.clone();
        runtime::spawn(async move {
            let group_id = group_info.id;
            // step 1: assign new group on this new
            let _ = nm.try_assign_group(group_info).await;
            if let Ok(peer) = nm.find_peer(group_id) {
                // step 2: remove outgoing voters by requsting leave joint.
                let _ = peer.propose_conf_changes_async(
                    ChangeSet::leave_joint().into(), 
                    false
                ).await;
            }
        });
    }
}
