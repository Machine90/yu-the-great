use crate::peer::config::NodeConfig;
use components::mailbox::topo::Topo;
use components::mailbox::{PostOffice};
use components::monitor::{Monitor, MonitorConf};
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

use super::driver::{CoprocessorDriver, AppliedTracker};
use super::{
    executor::{Coprocessor, Executor},
    listener::{Listener, Listeners},
};

pub struct Builder {
    coprocessor: Option<Executor>,
    listeners: Listeners,
    topo: Option<Topo>,
    monitor_conf: MonitorConf,
    pub(crate) conf: NodeConfig,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            coprocessor: None,
            listeners: Default::default(),
            monitor_conf: MonitorConf::default(),
            topo: None,
            conf: NodeConfig::default(),
        }
    }
}

impl Builder {
    pub fn from_conf(conf: NodeConfig) -> Self {
        Self {
            coprocessor: None,
            listeners: Default::default(),
            monitor_conf: MonitorConf::default(),
            topo: None,
            conf,
        }
    }

    #[allow(unused)]
    #[inline]
    pub fn listeners_num(&self) -> usize {
        self.listeners.len()
    }

    #[allow(unused)]
    #[inline]
    pub fn has_coprocessor(&self) -> bool {
        self.coprocessor.is_some()
    }

    #[inline]
    pub fn register_coprocessor(mut self, coprocessor: Coprocessor) -> Self {
        self.coprocessor = Some(Executor::new(coprocessor));
        self
    }

    #[inline]
    pub fn add_listener(self, listener: Listener) -> Self {
        self.listeners.add(listener);
        self
    }

    #[allow(unused)]
    #[inline]
    pub fn with_topology(mut self, topo: Topo) -> Self {
        self.topo = Some(topo);
        self
    }

    #[inline]
    pub fn config_monitor(mut self, conf: MonitorConf) -> Self {
        self.monitor_conf = conf;
        self
    }

    fn validate(&self) -> Result<()> {
        if self.coprocessor.is_none() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "coprocessor must be assign",
            ));
        }
        Ok(())
    }

    pub fn build(mut self, post_office: Arc<dyn PostOffice>) -> Result<CoprocessorDriver> {
        self.validate()?;

        let coprocessor = self
            .coprocessor
            .take()
            // not strictly, default to use delegation
            .unwrap();

        let listeners = std::mem::take(&mut self.listeners);
        let topo = self.topo.take().unwrap_or_default();

        let monitor = if self.monitor_conf.enable {
            Monitor::new(self.monitor_conf).ok()
        } else {
            None
        };

        let conf = self.conf;
        let endpoint = conf.get_endpoint()?;
        Ok(CoprocessorDriver {
            coprocessor,
            listeners: Arc::new(listeners),
            monitor,
            post_office,
            topo,
            endpoint,
            applied_tracker: AppliedTracker::default(),
            conf,
        })
    }
}
