use super::NodeCoordinator;
use crate::{
    coprocessor::listener::{admin::AdminListener, Acl, RaftContext},
    storage::group_storage::GroupStorage,
    warn,
};
use common::protos::multi_proto::{Assignment, BalanceCmd};
use components::{bincode::{self}, torrent::runtime};
use std::sync::Arc;

#[crate::async_trait]
impl<S: GroupStorage> Acl for NodeCoordinator<S> {
    #[inline]
    async fn accessible(&self, _: &RaftContext) -> bool {
        true
    }
}

#[crate::async_trait]
impl<S: GroupStorage + Clone> AdminListener for NodeCoordinator<S> {
    async fn handle_cmds(
        &self,
        _: &RaftContext,
        cmds: &Vec<Vec<u8>>,
        _raftstore: Option<Arc<dyn GroupStorage>>,
    ) {
        for cmd in cmds {
            let cmd = bincode::deserialize::<BalanceCmd>(cmd);
            if let Err(e) = cmd {
                warn!("{:?}", e);
                continue;
            }
            let cmd = cmd.unwrap();
            match cmd.assignment() {
                Assignment::Split => self.handle_split(cmd).await,
                Assignment::Create => {
                    let mut tx = self.manager.transaction().await;
                    for group in cmd.groups {
                        if tx.assign_group(group).is_err() {
                            let _ = tx.rollback().await;
                            return;
                        }
                    }
                    tx.commit().await;
                }
                Assignment::Compact => todo!(),
                Assignment::Transfer => todo!(),
            }
        }
    }
}

impl<S: GroupStorage + Clone> NodeCoordinator<S> {
    
    /// Handle all split commands that assigned to this node's group.
    pub(super) async fn handle_split(&self, split_cmd: BalanceCmd) {
        let mut tx = self.manager.transaction().await;
        let new_assignment = split_cmd.consumer;

        let mut advancer = split_cmd.advance();
        while let Some(mut left) = advancer.next_group() {
            let right = left.id;
            left.id = new_assignment;
            if tx.split_group(left, right).is_err() {
                tx.rollback().await;
                return;
            }
        }
        let to_clear = tx.commit().await;

        if !to_clear.is_empty() {
            let balancer = self.balancer.clone();
            let _handler = runtime::spawn_blocking(move || 
                balancer.clear_partitions(to_clear)
            );
        }
    }
}
