use super::NodeCoordinator;
use crate::{
    coprocessor::listener::{admin::AdminListener, Acl, RaftContext},
    peer::{facade::AbstractPeer, process::proposal::Proposal},
    storage::group_storage::GroupStorage,
};
use common::{
    errors::application::{Yusult, YuError},
    protocol::{GroupID, NodeID},
    protos::{
        multi_proto::{Assignments, BatchAssignment, Merge, Split, Transfer},
        raft_group_proto::GroupProto,
        raft_log_proto::ConfState,
    },
};
use components::{
    bincode::{self},
    mailbox::multi::balance::SplitPartition,
    torrent::{partitions::partition::Partition},
};
use std::{
    collections::{HashMap, HashSet},
    hash::Hash
};

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
        cmds: &Vec<Vec<u8>>
    ) {
        for cmd in cmds {
            let assignments = bincode::deserialize(cmd);
            if assignments.is_err() {
                continue;
            }
            // receive correct assignments and apply them at local.
            self.apply_assignments(assignments.unwrap()).await;
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Voters {
    members: HashSet<NodeID>,
}

impl Hash for Voters {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for n in self.members.iter() {
            n.hash(state);
        }
    }
}

#[derive(Default)]
struct Divided {
    assignments: Assignments,
    groups: Vec<GroupID>,
}

impl Divided {
    #[inline]
    fn add_split(&mut self, split: Split) {
        self.groups.push(split.src_group);
        self.assignments.should_split.push(split);
    }

    #[inline]
    fn add_merge(&mut self, merge: Merge) {
        self.groups.extend(&merge.src_groups);
        self.assignments.should_merge.push(merge);
    }
}

impl<S: GroupStorage + Clone> NodeCoordinator<S> {
    /// Processing assignments that received from federation on this node.
    /// The assignments will be propose to other effected voters.
    pub async fn process_assignments(&self, assignments: Assignments) {
        if assignments.is_empty() {
            return;
        }

        let topo = self.topo();

        let mut node_assigments = HashMap::new();
        // collect assignments which has same voters so that 
        // we can send them to the same node in batch.
        if !assignments.should_split.is_empty() {
            for split in assignments.should_split {
                let src_group = split.src_group;
                if let Some(members) = topo.copy_group_node_ids(&src_group) {
                    node_assigments
                        .entry(Voters { members })
                        .or_insert(Divided {
                            ..Default::default()
                        })
                        .add_split(split);
                }
            }
        }

        if !assignments.should_merge.is_empty() {
            for merge in assignments.should_merge {
                let src_group = merge.src_groups[0];
                if let Some(members) = topo.copy_group_node_ids(&src_group) {
                    node_assigments
                        .entry(Voters { members })
                        .or_insert(Divided {
                            ..Default::default()
                        })
                        .add_merge(merge);
                }
            }
        }

        if !assignments.should_transfer.is_empty() {
            // unlike split and compaction, transfer operation don't need
            // to propose to each voters, only handle it at local.
            self.handle_transfer(assignments.should_transfer).await;
        }

        if node_assigments.is_empty() {
            return;
        }

        for (voters, distinct) in node_assigments {
            let groups = distinct.groups;
            let batch = BatchAssignment {
                confstate: Some(voters.members.into()),
                assignments: Some(distinct.assignments),
            };
            let proposal = self._propose_assigments(groups, batch).await;
            if let Err(e) = proposal {
                crate::error!("failed to propose assignments, see: {:?}", e);
            }
        }
    }

    async fn _propose_assigments(&self, groups: Vec<GroupID>, mut batch: BatchAssignment) -> Yusult<()> {
        let propose_group = groups[0];
        let peer = self.manager.find_peer(propose_group)?;
        Self::_pre_propose(propose_group, &mut batch);
        let cmd = bincode::serialize(&batch);
        let proposal = peer.propose_cmd_async(cmd.unwrap()).await;
        if let Ok(proposal) = proposal {
            match proposal {
                Proposal::Commit(_) => {
                    return Ok(())
                },
                _ => ()
            }
        }
        Err(YuError::BalanceError)
    }

    fn _pre_propose(proposer: GroupID, batch: &mut BatchAssignment) {
        let BatchAssignment { assignments, .. } = batch;
        if let Some(assignments) = assignments {
            for merge in assignments.should_merge.iter_mut() {
                // keep the group which used to propose assignments,
                // and all groups merge to this group, so that un-replicated
                // raft node can sync with it's leader of this group later.
                merge.dest_group = proposer;
            }
        }
    }

    /// Apply assignment at local.
    pub(super) async fn apply_assignments(&self, mut batched: BatchAssignment) {
        let confstate = batched.confstate.take();

        if confstate.is_none() {
            crate::error!("not confstate for group assignments");
            return;
        }

        if batched.assignments.is_none() {
            return;
        }
        let assignments = batched.assignments.unwrap();
        if !assignments.should_split.is_empty() {
            self.handle_splits(assignments.should_split, confstate.clone()).await;
        }

        if !assignments.should_merge.is_empty() {
            self.handle_compaction(assignments.should_merge).await;
        }
    }

    /// Handle all split commands that assigned to this node's group.
    pub(super) async fn handle_splits(&self, splits: Vec<Split>, confstate: Option<ConfState>) {
        let mut split_partitions = Vec::new();
        for Split {
            src_group,
            dest_group,
            config_split_key,
        } in splits
        {
            let group = self.manager.find_peer(src_group);
            if group.is_err() {
                // skip this group.
                continue;
            }
            let (from, to) = group.unwrap().group_range();
            split_partitions.push(SplitPartition {
                new_group: dest_group,
                ori_partition: Partition::from_range(from..to, src_group),
                split_key: if config_split_key.is_empty() { 
                    None 
                } else {
                    Some(config_split_key)
                },
            });
        }

        // try get split keys for theses partitions
        self.balancer.split_keys(&mut split_partitions);

        // create a split transaction to do batch split.
        let mut split_tx = self.manager.transaction().await;
        let mut success = true;
        for SplitPartition {
            new_group,
            ori_partition:
                Partition {
                    from_key,
                    resident: origin_group,
                    ..
                },
            split_key,
        } in split_partitions
        {
            if split_key.is_none() {
                // couldn't find split key for partition.
                // ignore this group.
                continue;
            }
            let try_split = split_tx.split_group(
                origin_group,
                GroupProto {
                    id: new_group,
                    from_key: from_key.take(),
                    to_key: split_key.unwrap(),
                    confstate: confstate.clone(),
                    ..Default::default()
                },
            );
            if let Err(e) = try_split {
                success = false;
                crate::error!("failed to split group: {origin_group} because: {:?}", e);
                break;
            }
        }
        if success {
            split_tx.commit().await;
        } else {
            split_tx.rollback().await;
        }
    }

    async fn handle_compaction(&self, merges: Vec<Merge>) {
        let mut compact_tx = self.manager.transaction().await;

        let mut success = true;
        for Merge { src_groups, dest_group } in merges {
            let result = compact_tx
                .compaction(src_groups, dest_group).await;
            if let Err(e) = result {
                crate::error!("failed to compact groups to {dest_group} because: {:?}", e);
                success = false;
                break;
            }
        }
        if success {
            compact_tx.commit().await;
        } else {
            compact_tx.rollback().await;
        }
    }

    /// Transfer groups on this node to transfee, this operation 
    /// could be consider as: 
    /// * Transfer leadership to another follower by `transfer_leader`.
    /// * propose confchange `[Add(transfee), Remove(this node)]` to group.
    async fn handle_transfer(&self, transfers: Vec<Transfer>) {
        let this_node = self.node_id();
        for Transfer { group, transfee } in transfers {
            let inheritor = self._select_inheritor(group);
            if inheritor.is_none() || transfee.is_none() {
                continue;
            }
            let transfee = transfee.unwrap().try_into().unwrap();
            let mut transfer_tx = self.manager.transaction().await;
            let transfered = transfer_tx.transfer_to(
                inheritor.unwrap(), 
                group, 
                this_node, 
                transfee
            ).await;
            if transfered.is_ok() {
                transfer_tx.commit().await;
            } else {
                transfer_tx.rollback().await;
            }
        }
    }

    fn _select_inheritor(&self, group: GroupID) -> Option<NodeID> {
        let this_node = self.node_id();
        let inheritor = self.topo().get_group_mut(&group).map(|group| {
            let mut selected = None;
            group.scan_nodes(|node| {
                if node.id != this_node {
                    selected = Some(node.id);
                }
                node.id == this_node
            });
            selected
        })??;
        Some(inheritor)
    }
}
