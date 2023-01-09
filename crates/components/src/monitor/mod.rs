pub mod prober;

use std::{error::Error, ops::Deref};
use common::{metrics, protocol::GroupID};
use torrent::{dams::{Dam, Terminate}, runtime};

use self::prober::Prober;

#[derive(Debug, Clone, Copy)]
pub struct MonitorConf {
    pub enable: bool,
    /// keep sampling for a given past time in ms.
    pub statistic_dur_in_ms: u64,
    pub sampling_freqency: usize,
    pub enable_group_sampling: bool,
}

impl Default for MonitorConf {
    fn default() -> Self {
        Self {
            enable: true,
            statistic_dur_in_ms: 300000, // sampling for past 5min
            sampling_freqency: 60, // sampling each 5 seconds (statistic_dur_in_ms / sampling_freqency) / 1000
            enable_group_sampling: false
        }
    }
}

pub struct Monitor {
    terminate: Terminate,
    core: Dam,
    pub conf: MonitorConf
}

impl Deref for Monitor {
    type Target = Dam;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl Monitor {
    pub fn new(conf: MonitorConf) -> Result<Self, Box<dyn Error>> {
        let dam = Dam::from_timer(
            conf.statistic_dur_in_ms, // 1h, TODO: replace to configured value
            conf.sampling_freqency,   // sampling per 10s
        )?;
        Ok(Self {
            core: dam,
            terminate: Terminate::new(),
            conf
        })
    }

    pub async fn start(&self) {
        let term = &self.terminate;
        let task = self.core.forward(term);
        runtime::spawn_blocking(task);
    }

    #[inline(always)]
    pub fn stop(&self) {
        self.terminate.stop();
    }

    /// Create a prober using to do some sampling
    /// work in current window.
    pub fn probe<'a>(&'a self) -> Prober<'a> {
        Prober {
            window: self.core.window()
        }
    }

    /// Calculate the "Query rate per second" of current node.
    #[inline]
    pub fn qps(&self) -> f64 {
        self.core.avg(metrics::key::TOTAL_REQ) * 1000.
    }

    /// Calculate the "Query rate per second" of given group.
    #[inline]
    pub fn qps_of(&self, group: GroupID) -> f64 {
        self.core.avg(metrics::requests_of(group).as_str()) * 1000.
    }

    #[inline]
    pub fn thoughput(&self) -> f64 {
        self.core.avg(metrics::key::TOTAL_W_THROUGHPUT) * 1000.
    }

    #[inline]
    pub fn thoughput_of(&self, group: GroupID) -> f64 {
        self.core.avg(metrics::write_throughput_of(group).as_str()) * 1000.
    }

    /// Equal to Dam's `collect`, collect all sampling information from
    /// the monitor. 
    #[inline]
    pub fn statistics(&self) -> std::collections::HashMap<String, i64> {
        self.core.collect()
    }
}
