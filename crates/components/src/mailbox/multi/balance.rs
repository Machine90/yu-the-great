use super::model::check::CheckGroup;
use common::{protocol::GroupID, vendor::prelude::singleton};
use std::{sync::Arc, time::Duration, ops::Range};
use torrent::partitions::{key::Key, partition::Partition};

pub type NodeBalancer = Arc<dyn BalanceHelper + Send + Sync + 'static>;

/// Balance configuration.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub enable_schedule: bool,
    pub check_interval_millis: u64,
    // TODO: more split policy
    pub check_policy: CheckPolicy,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enable_schedule: true,
            check_interval_millis: 30000,
            check_policy: CheckPolicy::Percentage {
                min_group_percent: 0.00001,
                // assume a node has 1TB disk space, each group on this node
                // can hold atmost 1.024GB space, and we can have atmost 1000
                // such group.
                max_group_percent: 0.001,
            },
        }
    }
}

impl Config {
    #[inline]
    pub fn check_interval(&self) -> Duration {
        Duration::from_millis(self.check_interval_millis)
    }
}

singleton!(BALANCE_CONF, Config);

#[derive(Debug, Clone)]
pub struct SplitPartition {
    pub new_group: GroupID,
    pub ori_partition: Partition<GroupID>,
    pub split_key: Option<Vec<u8>>,
}

impl SplitPartition {

    #[inline]
    pub fn set_split_key<K: AsRef<[u8]>>(&mut self, key: K) {
        self.split_key = Some(key.as_ref().to_vec());
    }

    #[inline]
    pub fn take_key(&mut self) -> Option<Vec<u8>> {
        self.split_key.take()
    }
}

#[derive(Debug, Clone)]
pub enum Notification {
    ScaleUp {
        key_range: Range<Key>
    },
    ScaleDown { 
        key_range: Range<Key>
    },
    ChangeGroup {
        to: GroupID
    },
    ClearRange {
        group: GroupID,
        key_range: Range<Key>,
    }
}

/// Balance helper of current node, used to collect usages of each group
/// on this node, then help to find split_key of each group which need split.
#[allow(unused)]
pub trait BalanceHelper {
    /// Get the reference of balancer's configuration.
    fn conf(&self) -> &Config {
        BALANCE_CONF.get(|| {
            Config::default()
        })
    }

    /// Group Usage, use to check if group is too large to split
    /// or too small to compaction.
    fn groups_usage(&self, targets: &mut Vec<CheckGroup>);

    /// Get the split key of given groups
    fn split_keys(&self, should_splits: &mut Vec<SplitPartition>);

    /// Notify with balanced result to helper, the notification
    /// should be handled, for example received `ClearRange`, 
    /// then effected groups should do clear job themselves.
    /// 
    /// If want to run this job in async, then feel free to use
    /// `runtime::spawn()` `runtime::blocking()`. This method 
    /// is invoked on a tokio Runtime.
    /// 
    /// ### Params
    /// **events**: vector<(peer id, notification)>
    /// ### Noting that
    /// Assume this operation is always success, developer
    /// should handle all unexpected cases themselves, for example
    /// save events in WAL.
    fn notify(&self, events: Vec<(GroupID, Notification)>);
}

#[derive(Debug, Clone, Copy)]
pub enum CheckPolicy {
    Fixed {
        min_group_size: u64,
        max_group_size: u64,
    },
    Percentage {
        min_group_percent: f32,
        max_group_percent: f32,
    },
}
