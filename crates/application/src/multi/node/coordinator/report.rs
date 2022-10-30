use std::collections::HashSet;

use common::{protocol::GroupID, protos::multi_proto::{GroupInfo, MultiGroup}};
use components::storage::group_storage::GroupStorage;
use consensus::prelude::RaftRole;

use super::NodeCoordinator;


impl<S: GroupStorage + Clone> NodeCoordinator<S> {

    pub async fn report(&self, allow_empty: bool) {
        let to_report = &self.manager.report_list;
        let federation = self.federation.clone();
        if !allow_empty && (to_report.is_empty() || federation.is_none()) {
            return;
        }
        let groups: HashSet<GroupID> = to_report
            .iter()
            .map(|g| *g)
            .collect();
        
        let mut multi_group = MultiGroup::default();
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
                let (from, to) = maybe_leader.group_range();
                let group_info = GroupInfo {
                    id: group,
                    from_key: from.take(),
                    to_key: to.take(),
                    ..Default::default()
                };
                multi_group.groups.push(group_info);
            }
        }

        if !allow_empty && multi_group.groups.is_empty() {
            return;
        }
        self.topo().fill_groups(&mut multi_group);
        let federation = federation.unwrap();
        let assignments = federation.report_groups(self.node_id(), multi_group).await;
        self.process_assignments(assignments).await;
    }
}
