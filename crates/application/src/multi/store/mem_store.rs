use std::io::{Error, ErrorKind, Result};

use components::storage::multi_storage::{Filter, MultiStorage};

use crate::protos::raft_group_proto::GroupProto;
use crate::vendor::prelude::DashMap;
use crate::{
    store::memory::{storage_impl::MemoryStorage, store::PeerMemStore},
    GroupID,
};

/// The multiple memory storage maintain
/// all groups storage.
pub struct MultiMemStore {
    metainfo: DashMap<GroupID, GroupProto>,
}

impl MultiMemStore {
    pub fn new() -> Self {
        Self {
            metainfo: DashMap::new(),
        }
    }

    pub fn restore_from(groups: Vec<GroupProto>) -> Self {
        let metainfo = DashMap::with_capacity(groups.len());
        for group in groups {
            metainfo.insert(group.id, group);
        }
        let multi_store = Self { metainfo };
        multi_store
    }
}

impl MultiStorage<PeerMemStore> for MultiMemStore {
    fn get_group(&self, group_id: GroupID) -> Result<GroupProto> {
        self.metainfo
            .get(&group_id)
            .map(|group| group.clone())
            .ok_or(Error::new(ErrorKind::NotFound, "not exists group in store"))
    }

    fn list_peers(&self, filter: Filter, scanner: &mut dyn FnMut(GroupProto) -> bool) -> bool {
        for ent in self.metainfo.iter() {
            let group_id = *ent.key();
            if filter.do_filter(group_id) {
                continue;
            }
            let group = ent.value().clone();
            if !scanner(group) {
                return false;
            }
        }
        true
    }

    #[inline]
    fn build_store(&self, group: &mut GroupProto) -> Result<PeerMemStore> {
        let group_id = group.id;
        let confstate = group.confstate.take();
        let store = MemoryStorage::new();
        if let Some(confstate) = confstate {
            store.initialize_with_conf_state(confstate);
        }
        Ok(PeerMemStore::new(group_id, store))
    }

    #[inline]
    fn persist_group(&self, group: GroupProto) {
        self.metainfo.insert(group.id, group);
    }
}
