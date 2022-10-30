use protos::prelude::prost::Message;
use crate::protos::raft_log_proto::*;
use crate::errors::Result;

#[derive(Debug, Clone, Default)]
pub struct RaftState {
    pub hard_state: HardState,
    pub conf_state: ConfState
}

impl RaftState {
    pub fn new(hard_state: HardState, conf_state: ConfState) -> RaftState {
        RaftState {
            hard_state, conf_state
        }
    }

    #[inline]
    pub fn initialized(&self) -> bool {
        self.conf_state != ConfState::default()
    }
}

pub trait Storage: Send + Sync {

    fn initial_state(&self) -> Result<RaftState>;

    fn entries(&self, low: u64, high: u64, limit: Option<u64>) -> Result<Vec<Entry>>;

    fn term(&self, index: u64) -> Result<u64>;
    
    fn first_index(&self) -> Result<u64>;

    fn last_index(&self) -> Result<u64>;

    /// Prepare snapshot at `leader` side, then send it to 
    /// `follower`.
    fn snapshot(&self, index: u64) -> Result<Snapshot>;
}

pub const NO_LIMIT: u64 = u64::MAX;

pub fn limit_size<T: Message + Clone>(entries: &mut Vec<T>, max: Option<u64>) {
    if entries.len() <= 1 {
        return;
    }
    let max = match max {
        None | Some(NO_LIMIT) => return,
        Some(max) => max,
    };

    let mut size = 0;
    let limit = entries
        .iter()
        .take_while(|&_e| {
            if size == 0 {
                size += 1;
                true
            } else {
                size += 1;
                size <= max
            }
        })
        .count();

    entries.truncate(limit);
}

