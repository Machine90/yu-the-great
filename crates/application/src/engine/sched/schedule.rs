use std::{sync::Arc, time::Duration};

use components::vendor::prelude::{singleton};

singleton!(SCHED_CONF, ScheduleConf);

#[derive(Debug, Clone, Copy)]
pub struct ScheduleConf {
    /// Only available for async schedule.
    pub scheduling_timeout: Duration,
    pub should_parallel: bool,
    /// Only available when `should_parallel` is true.
    pub should_join: bool,
}

impl Default for ScheduleConf {
    fn default() -> Self {
        Self { 
            scheduling_timeout: Duration::from_millis(100), 
            should_parallel: false,
            should_join: true,
        }
    }
}

/// A Schedule is often used to do some job repeated, 
/// all impl of schedule that resgitered to `Scheduler`
/// will be triggered in interval.
/// ### Example
/// ```rust
/// pub struct Greeting(String);
/// impl Schedule for Greeting {
///     fn token(&self) -> &'static str {
///         "Greeting"
///     }
///     
///     fn nocked(&self, _tick: u64, _cfg: Arc<Configuration>) -> bool {
///         true
///     }
///     fn should_parallel(&self) -> bool {
///         true
///     }
///     fn fire(&self, tick: u64, _cfg: Arc<Configuration>) {
///         let period = tick % 3;
///         if period == 0 {
///             println!("Good morning {:?}!", self.0);
///         } else if period == 1 {
///             println!("Good afternoon {:?}!", self.0);
///         } else if period == 2 {
///             println!("Good evening {:?}!", self.0);
///         }
///     }
/// }
/// ```
pub trait Schedule: Send + Sync {

    /// Identify of this schedule, e.g. it's name.
    fn token(&self) -> &'static str;

    /// Is this scheduler should be fired in a async process.
    fn conf(&self) -> ScheduleConf {
        *SCHED_CONF.get(|| ScheduleConf::default())
    }

    /// Notify scheduler prepare to fire, and check if should fire,
    /// if return true, then `fire`
    fn nocked(&self, tick: u64) -> bool;

    /// Schedule logic. 
    fn fire(&self, tick: u64);
}

pub type RaftSchedule = Arc<dyn Schedule>;

/// An async Schedule is often used to do some job repeated, 
/// all impl of schedule that resgitered to `Scheduler`
/// will be triggered in interval.
/// 
/// ### Example
/// ```rust
/// pub struct Greeting(String);
/// 
/// #[async_trait]
/// impl Schedule for Greeting {
/// 
///     fn token(&self) -> &'static str {
///         "GreetingAsync"
///     }
/// 
///     async fn nocked(&self, _tick: u64) -> bool {
///         true
///     }
/// 
///     fn conf(&self) -> ScheduleConf {
///         ScheduleConf::default()
///     }
/// 
///     async fn fire(&self, tick: u64) {
///         let period = tick % 3;
///         if period == 0 {
///             println!("Good morning {:?}!", self.0);
///         } else if period == 1 {
///             println!("Good afternoon {:?}!", self.0);
///         } else if period == 2 {
///             println!("Good evening {:?}!", self.0);
///         }
///     }
/// }
/// ```
#[crate::async_trait]
pub trait ScheduleAsync: Send + Sync {

    /// Identify of this schedule, e.g. it's name.
    fn token(&self) -> &'static str;

    fn conf(&self) -> ScheduleConf {
        *SCHED_CONF.get(|| ScheduleConf::default())
    }

    /// Notify scheduler prepare to fire, and check if should fire,
    /// if return true, then `fire`
    async fn nocked(&self, tick: u64) -> bool;

    /// Schedule logic. 
    async fn fire(&self, tick: u64);
}

pub type RaftScheduleAsync = Arc<dyn ScheduleAsync + 'static>;