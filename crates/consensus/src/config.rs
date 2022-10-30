pub mod raft_config_vals {
    /// default 1
    pub const RAFT_PEER_ID: u64 = 1u64;
    /// default 10
    pub const RAFT_ELECTION_TIMEOUT: usize = 10;
    /// default 2
    pub const RAFT_HEARTBEAT_TIMEOUT: usize = 2;
    /// default false
    pub const RAFT_PRE_VOTE_ENABLE: bool = false;
    /// default to 7, use array (stack memo) rather than vec (heap memo)
    /// since elements number larger than this value
    pub const LIMITED_ALLOC_STK_ARR_SIZE: usize = 7;
    /// default true, then perform 2-phase (append, commit) commit in raft.
    pub const RAFT_BROADCAST_COMMIT_ENABLE: bool = true;

    pub const RAFT_BROADCAST_BECAME_LEADER_ENABLE: bool = true;

    pub const RAFT_MESSAGE_MAX_INFLIGHTS: usize = 256;

    pub const RAFT_RESTART_APPLIED_INDEX: u64 = 0;

    pub const RAFT_CHECK_QUORUM_ENABLE: bool = false;

    // default to 0, at most one entry in message.
    pub const RAFT_MAX_ENTRIES_PER_MESSAGE: u64 = 0;

    pub const RAFT_READ_BATCH_ENABLE: bool = false;
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// Node ID, normal use to mark current physical node.
    pub id: u64,
    pub tick_election_timeout: usize,
    pub tick_election_timeout_max: usize,
    pub tick_election_timeout_min: usize,
    pub tick_heartbeat_timeout: usize,
    pub enable_pre_vote_round: bool,
    pub should_check_quorum: bool,
    pub broadcast_commit_enable: bool,
    pub max_inflight_messages: usize,
    pub max_entries_size_per_message: u64,
    pub applied: u64,
    pub broadcast_became_leader: bool,
    pub enable_read_batch: bool,
}

impl Config {
    /// The minimum number of ticks before an election.
    #[inline]
    pub fn min_election_tick(&self) -> usize {
        if self.tick_election_timeout_min == 0 {
            self.tick_election_timeout
        } else {
            self.tick_election_timeout_min
        }
    }

    /// The maximum number of ticks before an election.
    #[inline]
    pub fn max_election_tick(&self) -> usize {
        if self.tick_election_timeout_max == 0 {
            2 * self.tick_election_timeout
        } else {
            self.tick_election_timeout_max
        }
    }
}

use raft_config_vals as vals;
impl Default for Config {
    #[inline]
    fn default() -> Self {
        Config {
            id: vals::RAFT_PEER_ID,
            tick_election_timeout: vals::RAFT_HEARTBEAT_TIMEOUT * 10,
            tick_election_timeout_min: 0,
            tick_election_timeout_max: 0,
            tick_heartbeat_timeout: vals::RAFT_HEARTBEAT_TIMEOUT,
            enable_pre_vote_round: vals::RAFT_PRE_VOTE_ENABLE,
            max_inflight_messages: vals::RAFT_MESSAGE_MAX_INFLIGHTS,
            applied: vals::RAFT_RESTART_APPLIED_INDEX,
            should_check_quorum: vals::RAFT_CHECK_QUORUM_ENABLE,
            max_entries_size_per_message: vals::RAFT_MAX_ENTRIES_PER_MESSAGE,
            broadcast_commit_enable: vals::RAFT_BROADCAST_COMMIT_ENABLE,
            broadcast_became_leader: vals::RAFT_BROADCAST_BECAME_LEADER_ENABLE,
            enable_read_batch: vals::RAFT_READ_BATCH_ENABLE,
        }
    }
}
