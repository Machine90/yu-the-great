use super::model::check::CheckGroup;
use common::protocol::GroupID;
use std::{collections::VecDeque, io, sync::Arc, time::Duration};
use torrent::partitions::key::Key;

pub type NodeBalancer = Arc<dyn BalanceHelper + Send + Sync + 'static>;

/// Balance configuration.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub check_interval_millis: u64,
    // TODO: more split policy
    pub check_policy: CheckPolicy,
}

impl Default for Config {
    fn default() -> Self {
        Self {
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

/// Balance helper of current node, used to collect usages of each group
/// on this node, then help to find split_key of each group which need split.
#[allow(unused)]
pub trait BalanceHelper {
    /// Get the reference of balancer's configuration.
    fn conf(&self) -> Config;

    /// Group Usage, use to check if group is too large to split
    /// or too small to compaction.
    fn groups_usage(&self, targets: &mut Vec<CheckGroup>);

    /// Get the split key of given group, this often called
    /// when after `check_split` is true.
    fn split_keys(&self, should_splits: &mut Vec<CheckGroup>);

    /// Notify to clear key-value in ranges of this node.
    /// ### Params
    /// **ranges**: vector<(peer id, from key, to key)>
    fn clear_partitions(&self, ranges: VecDeque<(GroupID, Key, Key)>) -> io::Result<()>;
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
