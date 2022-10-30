use super::group_storage::GroupStorage;
use crate::protos::{raft_group_proto::GroupProto};
use common::protocol::GroupID;
use std::collections::HashSet;
use std::{io::Result, sync::Arc};

pub type MultiStore<S> = Arc<dyn MultiStorage<S> + Send + Sync + 'static>;

/// The multi store used to manage all peer's info of this node.
pub trait MultiStorage<S: GroupStorage> {

    /// Assign group store to specific group on this node if not exists,
    /// otherwise get the already existed store and return.
    fn build_store(&self, group: &mut GroupProto) -> Result<S>;

    /// Persist metadata of group to storage, this supposed to 
    /// be always success.
    fn persist_group(&self, group: GroupProto);

    /// Both assign the store for group and persist metainfo if 
    /// success.
    fn assign_group(&self, mut group: GroupProto) -> Result<S>{
        let store = self.build_store(&mut group)?;
        self.persist_group(group);
        Ok(store)
    }

    fn list_peers(
        &self,
        filter: Filter,
        scanner: &mut dyn FnMut(GroupProto) -> bool,
    ) -> bool;

    fn get_group(&self, group_id: GroupID) -> Result<GroupProto>;
}

impl<GS: GroupStorage> dyn MultiStorage<GS> + Send + Sync + 'static {
    /// Scan peers from metadata storage with scanner,
    /// the peer_id and persist info will be passed in
    /// scanner, and if keep to scan, return true from scanner.
    pub fn scan_peers<S>(&self, filter: Filter, mut scanner: S) -> bool
    where
        S: FnMut(GroupProto) -> bool,
    {
        self.list_peers(filter, &mut scanner)
    }
}

#[derive(Default)]
pub struct Filter {
    pub in_groups: HashSet<GroupID>,
    pub not_in_groups: HashSet<GroupID>,
}

impl Filter {
    /// Not conditions
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_group(group: GroupID) -> Self {
        Self {
            in_groups: HashSet::from([group]),
            ..Default::default()
        }
    }

    /// filter this peer if return true
    #[inline]
    pub fn do_filter(&self, group: GroupID) -> bool {
        if !self.in_groups.is_empty() && !self.in_groups.contains(&group) {
            return true;
        }
        if !self.not_in_groups.is_empty() && self.not_in_groups.contains(&group) {
            return true;
        }
        false
    }
}
