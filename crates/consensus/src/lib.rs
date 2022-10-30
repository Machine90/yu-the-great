pub mod config;
pub mod raft;
pub mod raft_node;
use common::storage;

mod confchange;
mod quorum;

#[allow(unused)] use common::vendor::{error, warn, info, debug, trace, crit};
use common::protos;
use common::errors;

// declare the use name of quorum
use quorum::{joint::Joint, majority::Majority};
use raft::raft_tracker::configuration::Cluster;
use protos::prelude::prost;

/// In this situaion, we using fxhash for u64 type raft's id
pub type DefaultHashBuilder = std::hash::BuildHasherDefault<fxhash::FxHasher>;
pub type HashMap<K, V> = std::collections::HashMap<K, V, DefaultHashBuilder>;
pub type HashSet<K> = std::collections::HashSet<K, DefaultHashBuilder>;

pub mod prelude {
    pub use crate::{DefaultHashBuilder, HashMap, HashSet};
    pub use crate::{
        raft::{
            self, *, 
            raft_core::*,
            raft_log::*,
            raft_tracker::*,
            read_only::*,
            raft_role::*,
        }, 
        config as raft_config, 
        storage::*, 
        raft_node::{
            self, *,
            status::*,
            raft_functions::*,
            raft_process::*,
        },
    };
}
