use std::collections::HashSet;

use common::protocol::GroupID;
use components::storage::group_storage::GroupStorage;
use consensus::prelude::RaftRole;

use super::NodeCoordinator;


impl<S: GroupStorage> NodeCoordinator<S> {
    pub async fn report(&self) {
        let to_report = &self.manager.report_list;
        let federation = self.federation.clone();
        if to_report.is_empty() || federation.is_none() {
            return;
        }
        let groups: HashSet<GroupID> = to_report
            .iter()
            .map(|g| *g)
            .collect();
        
        for group in groups {
            if let Some(group) = to_report.remove(&group) {
                let maybe_leader = self.manager.peers
                    .get(&group);
                if maybe_leader.is_none() {
                    continue;
                }
                let maybe_leader = maybe_leader.unwrap();
                if maybe_leader.role().await != RaftRole::Leader {
                    continue;
                }
                let _group = maybe_leader.build_group().await;
            }
        }

        let _federation = federation.unwrap();
    }
}