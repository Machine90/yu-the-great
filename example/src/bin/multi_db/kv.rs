pub(crate) mod btree_engine;
pub(crate) mod hash_engine;

use std::{
    collections::{BTreeMap, VecDeque},
    sync::atomic::{AtomicU64, Ordering},
};

use application::{
    coprocessor::{
        listener::{proposal::RaftListener, Acl, RaftContext},
        ChangeReason,
    },
    RaftRole, SoftState,
};

use components::{
    bincode,
    common::protocol::read_state::ReadState,
    mailbox::multi::model::indexer::Unique,
    torrent::partitions::key::Key,
    vendor::prelude::{lock::RwLock, DashMap},
};
use serde::{Deserialize, Serialize};
use std::ops::Bound;

#[derive(Deserialize, Serialize)]
pub enum Operation {
    Put(Vec<u8>, Vec<u8>),
    Get(Vec<u8>),
}

impl Operation {
    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(key: K, value: V) -> Self {
        Self::Put(key.as_ref().to_vec(), value.as_ref().to_vec())
    }

    pub fn get<K: AsRef<[u8]>>(key: K) -> Self {
        Self::Get(key.as_ref().to_vec())
    }
}

impl Unique for Operation {
    fn get_index(&self) -> &[u8] {
        match self {
            Operation::Put(k, _) => &k[..],
            Operation::Get(k) => &k[..],
        }
    }
}
