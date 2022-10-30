use common::protocol::GroupID;

use crate::{
    mailbox::multi::balance::{CheckPolicy, Config},
    torrent::partitions::key::RefKey,
};

/// Perspective of "Usage", could be group or node, if group, then
/// total size and available space should be refer to each group's
/// storage.
///
/// **size**: The total size of the group's "storage".
/// **avail**: The amount of unused (free) space on the group's "storage".
#[derive(Debug, Clone, Copy)]
pub enum Perspective {
    Group { size: u64, avail: u64 },
    Node { size: u64 },
}

/// The usage of group, it's like `df -h` command
/// in `Linux`, use it to collect storage space.
#[derive(Debug, Clone, Copy)]
pub struct Usage {
    /// Amount of space used on each group's "storage". Max support 16TB.
    pub used: u64,
    /// Shows the percent of the group's "storage" used.
    pub percentage: f32,
    pub perspective: Perspective,
}

impl Usage {
    pub fn new(used: u64, perspective: Perspective) -> Self {
        // usg.percentage = used as f64 / size as f64;
        // let avail = if size >= used { size - used } else { 0 };
        let percentage = match perspective {
            Perspective::Group { size, .. } => used as f32 / size as f32,
            Perspective::Node { size, .. } => used as f32 / size as f32,
        };
        Self {
            used,
            percentage,
            perspective,
        }
    }
}

#[derive(Debug, Eq)]
pub enum Suggestion {
    Split {
        limit_bytes: u64,
        split_key: Option<Vec<u8>>,
    },
    Compact,
    Keep,
    Unchecked,
}

impl PartialEq for Suggestion {
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::match_same_arms))]
    fn eq(&self, other: &Suggestion) -> bool {
        match (self, other) {
            (Suggestion::Split { .. }, Suggestion::Split { .. }) => true,
            (Suggestion::Compact, Suggestion::Compact) => true,
            (Suggestion::Keep, Suggestion::Keep) => true,
            (Suggestion::Unchecked, Suggestion::Unchecked) => true,
            _ => false,
        }
    }
}

impl Suggestion {
    pub fn is_split(&self) -> bool {
        match self {
            Suggestion::Split { .. } => true,
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct CheckGroup<'a> {
    pub id: GroupID,
    pub from: RefKey<'a>,
    pub to: RefKey<'a>,
    pub gu: Option<Usage>,
    pub suggestion: Suggestion,
}

impl CheckGroup<'_> {
    pub fn get_split_size(&self) -> Option<u64> {
        match &self.suggestion {
            Suggestion::Split { limit_bytes, .. } => Some(*limit_bytes),
            _ => None,
        }
    }

    #[inline]
    pub fn set_split_key<K: AsRef<[u8]>>(&mut self, key: K) -> bool {
        match &mut self.suggestion {
            Suggestion::Split { split_key, .. } => {
                split_key.replace(key.as_ref().to_vec());
                true
            }
            _ => false,
        }
    }
}

impl CheckGroup<'_> {
    pub fn check_balance(&mut self, conf: &Config) {
        let Self { gu, .. } = self;
        if gu.is_none() {
            return;
        }
        let gu = gu.unwrap();
        let suggestion = match conf.check_policy {
            CheckPolicy::Fixed {
                min_group_size,
                max_group_size,
            } => {
                if gu.used >= max_group_size {
                    Suggestion::Split {
                        split_key: None,
                        limit_bytes: gu.used / 2,
                    }
                } else if gu.used < min_group_size {
                    Suggestion::Compact
                } else {
                    Suggestion::Keep
                }
            }
            CheckPolicy::Percentage {
                min_group_percent,
                max_group_percent,
            } => {
                if gu.percentage >= max_group_percent {
                    Suggestion::Split {
                        split_key: None,
                        limit_bytes: gu.used / 2,
                    }
                } else if gu.percentage < min_group_percent {
                    Suggestion::Compact
                } else {
                    Suggestion::Keep
                }
            }
        };
        self.suggestion = suggestion;
    }
}
