pub mod facade;
pub mod schedules;

use crate::{
    engine::sched::scheduler::Scheduler,
    peer::{facade::local::LocalPeer, Peer},
    RaftResult,
};
use components::torrent::runtime;
use std::{ops::Deref, sync::Arc};

/// Single raft group runing on a Node (normally physical machine)
pub struct Node {
    pub core: Arc<Core>,
}

pub struct Core {
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) peer: Arc<Peer>,
}

impl Node {
    /// Get a local client from `Node`, this
    /// client can access raft API without
    /// IO transport. The API see
    /// [RaftClient](crate::mailbox::api::RaftClient)
    #[inline]
    pub fn get_local_client(&self) -> LocalPeer {
        LocalPeer {
            peer: self.peer.clone(),
        }
    }
}

impl Core {
    #[inline]
    pub async fn start(&self) -> RaftResult<()> {
        self.coprocessor_driver().start().await?;
        self.scheduler.start();
        Ok(())
    }

    #[inline]
    pub async fn stop(&self) -> RaftResult<()> {
        self.coprocessor_driver().stop().await?;
        self.scheduler.stop();
        Ok(())
    }
}

impl Deref for Node {
    type Target = Core;

    fn deref(&self) -> &Self::Target {
        self.core.as_ref()
    }
}

impl Deref for Core {
    type Target = Peer;

    fn deref(&self) -> &Self::Target {
        self.peer.as_ref()
    }
}

impl Drop for Core {
    fn drop(&mut self) {
        runtime::blocking(async move {
            let _ = self.stop().await;
        });
    }
}

#[allow(unused)]
mod tests;
