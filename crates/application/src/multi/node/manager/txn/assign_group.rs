use crate::{multi::node::manager::peer_assigner::Moditication, GroupID, peer::facade::AbstractPeer};

use super::Transaction;
use common::{
    errors::application::{YuError, Yusult},
    protos::{raft_group_proto::GroupProto}, protocol::NodeID,
};
use components::{
    storage::group_storage::GroupStorage,
    torrent::partitions::{key::Key, partition::Partition}, mailbox::RaftEndpoint, utils::endpoint_change::{ChangeSet, EndpointChange},
};

impl<S: GroupStorage + Clone> Transaction<S> {

    /// Split a large partition by using left, then remove this separated
    /// partition if this node is not a voter of it.
    /// ### Example
    /// ```rust
    /// // the partitions on this node(1) looks like:
    /// let partitions = {
    ///     "dd": { from: "aa", to: "dd", group: 1 },
    ///     "hh": { from: "dd", to: "hh", group: 2 },
    ///     "zz": { from: "kk", to: "zz", group: 3 }
    /// };
    /// 
    /// // case 1: split and remove.
    /// assert!(txn.split_group(GroupProto {
    ///     id: 4,
    ///     from_key: b"aa".to_vec(),
    ///     to_key: b"cc".to_vec(),
    ///     // current node 1 is not in voters. 
    ///     confstate: Some([2,3,4].into()),
    ///     ..Default::default()
    /// }).is_ok());
    /// 
    /// // after split:
    /// let partitions = {
    ///     "dd": { from: "cc", to: "dd", group: 1 },
    ///     "hh": { from: "dd", to: "hh", group: 2 },
    ///     "zz": { from: "kk", to: "zz", group: 3 }
    /// }; 
    /// 
    /// // case 2: only split
    /// assert!(txn.split_group(GroupProto {
    ///     id: 4,
    ///     from_key: b"aa".to_vec(),
    ///     to_key: b"cc".to_vec(),
    ///     // current node 1 is one of voters. 
    ///     confstate: Some([1,3,4].into()),
    ///     ..Default::default()
    /// }).is_ok());
    /// 
    /// // after split:
    /// let partitions = {
    ///     "cc": { from: "aa", to: "cc", group: 4 },
    ///     "dd": { from: "cc", to: "dd", group: 1 },
    ///     "hh": { from: "dd", to: "hh", group: 2 },
    ///     "zz": { from: "kk", to: "zz", group: 3 }
    /// }; 
    /// ```
    pub fn split_group(&mut self, origin_group: GroupID, left: GroupProto) -> Yusult<()> {
        self.trace("prepare split group");
        // make sure origin group is exists on this node.
        if !self.peers.contains_key(&origin_group) {
            return Err(YuError::BalanceError(
                format!("group-{origin_group} is not exist on this node").into()
            ));
        }
        let new = Partition::new_and_validate(
            Key::left(&left.from_key),
            Key::right(&left.to_key),
            left.id,
        )?;

        if !self.version.maybe_add(new.clone(), true) {
            // maybe failure when other version is editing, or add an unexpected partition
            return Err(YuError::BalanceError(
                format!("unable to split group-{} to {}, make sure it has valid key-range, maybe other txns edit it in concurrency", origin_group, left.id).into()
            ));
        }

        // otherwise create a new raft peer for this node.
        let group_id = left.id;

        let new_from = Key::left(&left.to_key);
        self.peer_assigner
            .new_peer(left)
            .and_then(|created| {
                self.modification
                    .add(group_id, Moditication::Insert(created))
                    .add(origin_group, Moditication::ScaleDown { new_from }
                );
                Ok(())
            })?;
        Ok(())
    }

    /// Assign the group on this node.
    pub fn assign_group(&mut self, group: GroupProto) -> Yusult<()> {
        self.trace("prepare assign group");
        let node_id = self.peer_assigner.node_id();
        if !group.is_voter(node_id) {
            // should not assign on this node.
            return Err(YuError::BalanceError(
                format!("node-{node_id} is not a voter of group-{}", group.id).into()
            ));
        }
        let GroupProto {
            id,
            from_key,
            to_key,
            confstate,
            ..
        } = &group;
        if confstate.is_none() {
            crate::warn!("conf_state of group should never be None when assign");
            return Err(YuError::BalanceError("confstate should not be None".into()));
        }
        
        let group_id = *id;

        // case 2: try add or split group as left.
        let new = Partition::new_and_validate(
            Key::left(from_key), 
            Key::right(to_key), 
            group_id
        )?;

        if !self.version.maybe_add(new, true) {
            // maybe failure when other version is editing, or add an unexpected partition
            return Err(YuError::BalanceError(format!("unable to add group: {} to partitions", id).into()));
        }

        // try to assign the peer of group.
        self.peer_assigner
            .new_peer(group)
            .and_then(|created| {
                self.modification.add(group_id, Moditication::Insert(created));
                Ok(())
            })?;
        self.trace("assign group succeed");
        Ok(())
    }

    pub async fn compaction(&mut self, pieces: Vec<GroupID>, new_group: GroupID) -> Yusult<()> {
        if pieces.len() < 2 {
            return Err(YuError::BalanceError(
                "number of groups that prepare to merge should be at least 2".into()
            ));
        }
        let mut to_compact = Vec::with_capacity(pieces.len());
        for group in pieces {
            let peer = self.peers
                .get(&group)
                .ok_or(YuError::BalanceError(
                    format!("group-{group} is not exist on this node").into()
                ))?;
            let (from, to) = peer.group_range();
            to_compact.push(Partition::from_range(from..to, group));
        }
        // the groups prepare to merge must be adjanced with each others
        Partition::sort(&mut to_compact);
        // compact serveral smapll groups to new. 
        self.version.try_compact(&to_compact, new_group, true)?;

        // merge success, then these groups should be removed from peers.
        for group in to_compact.iter() {
            let group_id = group.resident;
            if group_id == new_group {
                continue;
            }
            self.modification.add(group_id, Moditication::MergeTo(new_group));
        }

        let first = to_compact.remove(0);
        let last = to_compact.remove(to_compact.len() - 1);
        self.modification.add(new_group, Moditication::ScaleUp { 
            new_from: first.from_key, 
            new_to: last.to_key 
        });
        Ok(())
    }

    /// Try to transfer given group from source node 
    /// to dest node.
    pub async fn transfer_to(
        &mut self, 
        new_leader: NodeID, 
        group_id: GroupID, 
        src: NodeID, 
        transfee: RaftEndpoint
    ) -> Yusult<()> {
        self.trace(format!(
            "transfer group-{} of node {} to {}", 
            group_id, src, transfee.id
        ).as_str());

        let peer = self.peers
            .get(&group_id)
            .ok_or(YuError::BalanceError(
                format!("group-{group_id} is not exist on this node").into()
            ))?;

        let (clear_from, clear_to) = peer.group_range();

        let part = Partition::new(
            clear_from, 
            clear_to, 
            group_id
        );
        // step 1: try remove partition of group on this node in current version.
        self.version.try_remove(&part, true)?;

        if peer.leader_id().await == src {
            // step 2: it's ok to transfer leader first, if current transaction
            // failed, it has not effect on this node.
            self.trace(format!("transfer leader to {}", new_leader).as_str());
            peer.transfer_leader_async(new_leader).await?;
        }
        let confchange = ChangeSet::new()
            .add(EndpointChange::add_as_voter(&transfee))
            .add(EndpointChange::remove(src));
        // step 3: propose changes remove current node and add new node to 
        // given group.
        let _ = peer.propose_conf_changes_async(confchange.into(), false).await?;
        self.modification.add(group_id, Moditication::Remove);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use common::{errors::application::Yusult, protos::raft_group_proto::GroupProto};
    use components::{
        torrent::partitions::index::{mvcc::{version::Version, Mvcc, timeline::Timeline}, sparse::Sparse},
    };

    struct Groups {
        mvcc: Mvcc<GroupID>,
    }

    impl Groups {
        pub fn new() -> Self {
            let mut index = Sparse::default();
            index.maybe_add(
                Partition::from_range("aa".."ee", 1)
            );
            index.maybe_add(
                Partition::from_range("ee".."kk", 2)
            );
            index.maybe_add(
                Partition::from_range("oo".."tt", 3)
            );
            index.maybe_add(
                Partition::from_range("vv".."zz", 4)
            );
            Self { mvcc: Mvcc::from_timeline(Timeline::from_exists(index)) }
        }

        pub fn txn(&self) -> Txn {
            let version = self.mvcc.new_version();
            Txn { node: 1, version }
        }
    }

    struct Txn {
        node: u64,
        version: Version<GroupID>,
    }

    impl Txn {
        fn split_group(&mut self, left: GroupProto) -> Yusult<()> {
            let this_node = self.node;
            let new = Partition::new_and_validate(
                Key::left(&left.from_key),
                Key::right(&left.to_key),
                left.id,
            )?;
            if !self.version.maybe_add(new.clone(), true) {
                // maybe failure when other version is editing, or add an unexpected partition
                return Err(YuError::BalanceError(
                    format!("unable to split group-{}, make sure it has valid key-range, maybe other txns edit it in concurrency", left.id).into()
                ));
            }
            if !left.is_voter(this_node) {
                // this node is not in split group, indicate that should
                // remove group from this node, but keep the divided one.
                if !self.version.try_remove(&new, true)? {
                    return Err(YuError::BalanceError(
                        format!("unable to remove group-{} from this node when splitting, maybe other txns edit it in concurrency", left.id).into()
                    ));
                }
            }
            Ok(())
        }
    }

    #[test]
    fn test_split_group() {
        let groups = Groups::new();
        let mut txn = groups.txn();
        let (f, t) = groups.mvcc.current()
            .estimate_split("cc").unwrap();
        let _r = txn.split_group(GroupProto { 
            id: 5, 
            from_key: f.take(), 
            to_key: t.take(), 
            confstate: Some(vec![2, 3, 4].into()),
            ..Default::default() 
        });
        txn.version.commit();
        assert!(groups.mvcc.current().list().count() == 4);
    }
}
