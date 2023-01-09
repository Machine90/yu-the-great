
use std::{time::Duration, ops::{Deref, DerefMut}};

use crate::{NodeID, RaftConfig, mailbox::{RaftEndpoint, LABEL_MAILBOX}};

#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// when follower recv redirect (proposal or read) from other follower.
    /// Maybe this follower is old leader before.
    pub allow_forward_spread: bool,
    pub max_wait_append_duration: u64,
    pub host: String,
    /// it's often means RPC port.
    pub mailbox_port: u16,
    /// configurations for multi-groups, warm up groups
    /// on a node after loaded them.
    pub preheat_groups_percentage: f32,
    pub preheat_groups_retries: u32,
    pub preheat_allow_failure: bool,
    /// Default to 1s, current process will wait and receive backup 
    /// of snapshot in place (in timeout), if backup small enough, 
    /// the process will finish immediately, otherwise do it in the 
    /// future by another coroutine.
    pub wait_backup_tranfer_ms: u64,
    /// Default to `true`, in theory, Raft algorithm would not to handle if 
    /// applying LogEntry failed, for exmaple disk unavailable suddenly. 
    /// But if disable this option, the failure will be catched and persistence 
    /// last applied index, then shutdown node, the entries since applied + 1
    /// will be continue to apply after restart.
    pub apply_ignore_failure: bool,
    /// Default to 100, means persistence `last_applied` index each 100 increments.
    pub apply_persistence_index_frequency: u64,
    /// Default to 100, every persistence `last_applied` increase 1 count, when count 
    /// reached `apply_compact_logs_frequency`, then trigger to clear raft logs. 
    /// For example `apply_persist_index_frequency` is 100, the raft log will clear each
    /// 10000 applied entries.
    pub apply_clear_logs_frequency: u32,
    /// consensus algorithm related-config, not changeable.
    pub consensus_config: RaftConfig,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self { 
            allow_forward_spread: false, 
            max_wait_append_duration: 100, 
            host: "localhost".to_owned(), 
            mailbox_port: 50010, 
            preheat_groups_percentage: 0.5,
            preheat_groups_retries: 5,
            preheat_allow_failure: true,
            wait_backup_tranfer_ms: 1000,
            apply_ignore_failure: true,
            apply_persistence_index_frequency: 100,
            apply_clear_logs_frequency: 100,
            consensus_config: Default::default() 
        }
    }
}

impl Deref for NodeConfig {
    type Target = RaftConfig;

    fn deref(&self) -> &Self::Target {
        &self.consensus_config
    }
}

impl DerefMut for NodeConfig {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.consensus_config
    }
}

impl NodeConfig {

    pub fn from_address(node_id: NodeID, host: &str, port: u16) -> Self {
        let mut consensus_config = RaftConfig::default();
        consensus_config.id = node_id;
        Self { host: host.to_owned(), mailbox_port: port, consensus_config, ..Default::default() }
    }

    #[inline]
    pub fn consensus(&self) -> &RaftConfig {
        &self.consensus_config
    }

    #[inline]
    pub fn max_wait_append_duration(&self) -> Duration {
        Duration::from_millis(self.max_wait_append_duration)
    }

    pub fn set_address(&mut self, address: &str) {
        let port_token = address.find(":");
        if let Some(port_token) = port_token {
            let host = &address[0..port_token];
            self.host = host.to_owned();
            let port = &address[port_token + 1..];
            let port = port.parse::<u16>();
            if let Ok(port) = port {
                self.mailbox_port = port;
            }
        }
    }

    /// Get endpoint of current peer's mailbox via configuration.
    #[inline] pub fn get_endpoint(&self) -> std::io::Result<RaftEndpoint> {
        // TODO: if more ports are present, add them to endpoint and return.
        RaftEndpoint::new(
            self.id, 
            LABEL_MAILBOX, 
            format!("{}:{}", self.host, self.mailbox_port).as_str()
        )
    }

    #[inline]
    pub fn max_wait_backup_tranfer_duration(&self) -> Duration {
        let wait_backup_tranfer_ms = std::cmp::max(self.wait_backup_tranfer_ms, 500);
        // between 500ms to 30s
        Duration::from_millis(std::cmp::min(wait_backup_tranfer_ms, 30000))
    }

    #[inline]
    pub fn groups_warmup_percentage(&self) -> f32 {
        let percentage = self.preheat_groups_percentage;
        if percentage > 1. {
            1.
        } else {
            percentage
        }
    }
}

impl Into<RaftConfig> for &NodeConfig {
    fn into(self) -> RaftConfig {
        self.consensus_config.clone()
    }
}
