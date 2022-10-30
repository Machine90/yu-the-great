use crate::{multi::node::manager::peer_assigner::Moditication, GroupID};

use super::Transaction;
use common::{
    errors::application::{YuError, Yusult},
    protos::raft_group_proto::GroupProto,
};
use components::{
    storage::group_storage::GroupStorage,
    torrent::partitions::{key::Key, partition::Partition},
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
    pub fn split_group(&mut self, left: GroupProto, origin: GroupID) -> Yusult<()> {
        self.trace("prepare split group");
        let this_node = self.peer_assigner.node_id();
        let new = Partition::new_and_validate(
            Key::left(&left.from_key),
            Key::right(&left.to_key),
            left.id,
        )?;

        if !self.version.maybe_add(new.clone(), true) {
            // maybe failure when other version is editing, or add an unexpected partition
            return Err(YuError::BalanceError);
        }

        if !left.is_voter(this_node) {
            self.trace("prepare remove split group");
            // this node is not in split group, indicate that should
            // remove group from this node, but keep the divided one.
            if !self.version.try_remove(&new, true)? {
                return Err(YuError::BalanceError);
            }
            self.modification.insert(origin, Moditication::ClearRange { 
                from: new.from_key, 
                to: new.to_key 
            });
        } else {
            // otherwise create a new raft peer for this node.
            let group_id = left.id;
            self.peer_assigner
                .new_peer(left)
                .and_then(|created| {
                    self.modification.insert(group_id, Moditication::Insert(created));
                    Ok(())
                })?;
        }
        Ok(())
    }

    /// Assign the group on this node.
    pub fn assign_group(&mut self, group: GroupProto) -> Yusult<()> {
        self.trace("prepare assign group");
        let node_id = self.peer_assigner.node_id();
        if !group.is_voter(node_id) {
            // TODO: should not assign on this node.
            return Err(YuError::BalanceError);
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
            return Err(YuError::BalanceError);
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
            return Err(YuError::BalanceError);
        }

        // try to assign the peer of group.
        self.peer_assigner
            .new_peer(group)
            .and_then(|created| {
                self.modification.insert(group_id, Moditication::Insert(created));
                Ok(())
            })?;
        self.trace("assign group succeed");
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
                return Err(YuError::BalanceError);
            }
            if !left.is_voter(this_node) {
                // this node is not in split group, indicate that should
                // remove group from this node, but keep the divided one.
                if !self.version.try_remove(&new, true)? {
                    return Err(YuError::BalanceError);
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
