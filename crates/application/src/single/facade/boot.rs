use std::{hint::spin_loop, sync::Arc};

use common::{protocol::GroupID, protos::raft_group_proto::GroupProto};
use components::mailbox::api::NodeMailbox;

use crate::{
    peer::{
        facade::{
            local::LocalPeer,
            mailbox::{GroupManage, MailboxService},
        },
        Peer,
    },
    single::Node,
    tokio::runtime::Runtime,
    ConsensusError, RaftResult,
};

impl Node {
    fn _tick_pool() -> std::io::Result<Runtime> {
        let rt = crate::tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("single-raft-ticker")
            .build();
        Ok(rt.unwrap())
    }

    pub fn start<R, Output>(&self, runner: R) -> std::io::Result<Output>
    where
        R: FnOnce(Arc<dyn NodeMailbox>) -> Output,
    {
        let core = self.core.clone();

        // let signal = self.terminate.clone();
        let peer = self.peer.clone();
        std::thread::spawn(move || {
            let ticker_pool = Self::_tick_pool();
            if let Err(e) = ticker_pool {
                crate::error!("failed to boot ticker on current thread, because: {e}");
                return;
            }
            ticker_pool.unwrap().block_on(async move {
                let _r = core.start().await;
                loop {
                    spin_loop();
                }
            });
        });

        let serivce: Arc<dyn NodeMailbox> = Arc::new(MailboxService {
            manager: Arc::new(SingleMailbox { peer }),
        });
        Ok(runner(serivce))
    }
}

struct SingleMailbox {
    peer: Arc<Peer>,
}

impl GroupManage for SingleMailbox {
    #[inline]
    fn get_group(&self, group: GroupID) -> RaftResult<LocalPeer> {
        if self.peer.get_group_id() != group {
            return Err(ConsensusError::StepPeerNotFound);
        }
        let peer = LocalPeer {
            peer: self.peer.clone(),
        };
        Ok(peer)
    }

    fn create_group(&self, _: GroupProto) {
        crate::debug!("we don't support create group on node in single group mode");
    }
}
