use crate::{info, warn, tokio::time::Instant,};
use components::{
    
    vendor::prelude::{
        lock::{RwLock, RwLockWriteGuard}
    },
};
use std::{
    collections::HashMap,
    io::{Result},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use super::{
    schedule::{RaftSchedule, RaftScheduleAsync, Schedule, ScheduleAsync},
    worker::{AsyncWorker, Worker},
};

pub struct Scheduler {
    runing: Arc<AtomicBool>,
    procedure: RwLock<Procedure>,
    tick_frequency: Duration,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self { 
            runing: Default::default(), 
            procedure: Default::default(), 
            // 10 times per second
            tick_frequency: Duration::from_millis(100) 
        }
    }
}

impl Scheduler {
    pub fn new(tick_frequency_millis: u64) -> Self {
        Self::with_procedure(Procedure::new(), tick_frequency_millis)
    }

    /// create scheduler with exists procedure, and tick it in 
    /// specific frequency (by using `tfim` "tick_frequency_in_millis")
    pub fn with_procedure(procedure: Procedure, tfim: u64) -> Self {
        Self {
            runing: Arc::new(AtomicBool::new(false)),
            procedure: RwLock::new(procedure),
            tick_frequency: Duration::from_millis(tfim)
        }
    }

    #[inline]
    pub fn is_runing(&self) -> bool {
        self.runing.load(Ordering::Relaxed)
    }

    pub fn procedure<V>(&self, visitor: V)
    where
        V: FnOnce(RwLockWriteGuard<Procedure>),
    {
        let procedure = self.procedure.write();
        visitor(procedure);
    }

    #[inline]
    pub fn add(&mut self, schedule: RaftSchedule) -> Result<()> {
        self.procedure.get_mut().add(schedule)
    }

    #[inline]
    pub fn spawn<S>(&mut self, schedule: S) -> Result<()>
    where
        S: Schedule + 'static,
    {
        self.procedure.get_mut().spawn(schedule)
    }

    #[inline]
    pub fn add_async(&mut self, schedule: RaftScheduleAsync) -> Result<()> {
        self.procedure.get_mut().add_async(schedule)
    }

    #[inline]
    pub fn spawn_async<S>(&mut self, schedule: S) -> Result<()>
    where
        S: ScheduleAsync + 'static,
    {
        self.procedure.get_mut().spawn_async(schedule)
    }

    pub fn start(&self) {
        if self.procedure.read().track().is_empty() {
            warn!("[Scheduler] not schedule registered!");
            return;
        }

        let try_running = self.runing.compare_exchange(
            false,
            true,
            Ordering::Acquire,
            Ordering::Relaxed,
        );
        if let Err(_) = try_running {
            warn!("[Scheduler] already ran, please stop it first!");
            return;
        }
        
        let mut procedure = self.procedure.write();
        info!("[Scheduler] start scheduling {:?} tasks", procedure.track().total_schedules());
        procedure._start(self.runing.clone(), self.tick_frequency);
    }

    pub fn stop(&self) {
        let stopped =
            self.runing
                .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed);
        if let Ok(true) = stopped {
            let mut procedure = self.procedure.write();
            info!("[Scheduler] stop scheduling {:?}", procedure.track());
            procedure._stop();
        }
    }

    pub fn restart(&self) {
        self.procedure(|mut procedure| {
            procedure._restart(self.runing.clone(), self.tick_frequency);
        });
    }
}

#[derive(Debug)]
pub struct Progress {
    pub scheduled_num_async: usize,
    pub prepared_num_async: usize,
    pub scheduled_num: usize,
    pub prepared_num: usize,
    pub scheduled_dur: Duration,
    pub scheduled_dur_async: Duration,
}

impl Progress {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.total_schedules() == 0
    }

    #[inline]
    pub fn total_schedules(&self) -> usize {
        self.scheduled_num 
            + self.prepared_num 
            + self.scheduled_num_async 
            + self.prepared_num_async
    }
}

#[derive(Default)]
pub struct Procedure {
    pub schedules: HashMap<&'static str, RaftSchedule>,
    pub async_schedules: HashMap<&'static str, RaftScheduleAsync>,
    worker: Option<Worker>,
    async_worker: Option<AsyncWorker>,
}

impl Procedure {
    pub fn new() -> Self {
        Self {
            schedules: HashMap::new(),
            async_schedules: HashMap::new(),
            worker: None,
            async_worker: None,
        }
    }

    #[inline]
    pub fn is_runing(&self) -> bool {
        self.worker.is_some() || self.async_worker.is_some()
    }

    pub fn track(&self) -> Progress {
        let now = Instant::now();

        let (scheduled_num, scheduled_dur) = self
            .worker
            .as_ref()
            .map(|w| (w.offered, now.duration_since(w.start_time)))
            .unwrap_or((0, Duration::ZERO));

        let (scheduled_num_async, scheduled_dur_async) = self
            .async_worker
            .as_ref()
            .map(|w| (w.offered, now.duration_since(w.start_time)))
            .unwrap_or((0, Duration::ZERO));

        Progress {
            scheduled_num_async,
            prepared_num_async: self.async_schedules.len(),
            scheduled_num,
            prepared_num: self.schedules.len(),
            scheduled_dur,
            scheduled_dur_async,
        }
    }

    #[inline]
    pub fn add(&mut self, schedule: RaftSchedule) -> Result<()> {
        let token = schedule.token();
        self.schedules.insert(token, schedule);
        Ok(())
    }

    #[inline]
    pub fn spawn<S>(&mut self, schedule: S) -> Result<()>
    where
        S: Schedule + 'static,
    {
        let token = schedule.token();
        self.schedules.insert(token, Arc::new(schedule));
        Ok(())
    }

    #[inline]
    pub fn add_async(&mut self, schedule: RaftScheduleAsync) -> Result<()> {
        let token = schedule.token();
        self.async_schedules.insert(token, schedule);
        Ok(())
    }

    #[inline]
    pub fn spawn_async<S>(&mut self, schedule: S) -> Result<()>
    where
        S: ScheduleAsync + 'static,
    {
        let token = schedule.token();
        self.async_schedules.insert(token, Arc::new(schedule));
        Ok(())
    }

    fn _start(&mut self, signal: Arc<AtomicBool>, tick_frequecy: Duration) {
        if !self.schedules.is_empty() {
            let schedules = std::mem::take(&mut self.schedules);
            let mut worker = Worker::default();
            worker.offer(signal.clone(), tick_frequecy, schedules);
            self.worker = Some(worker);
        }

        if !self.async_schedules.is_empty() {
            let schedules = std::mem::take(
                &mut self.async_schedules
            );
            let mut worker = AsyncWorker::default();
            worker.offer(signal, tick_frequecy, schedules);
            self.async_worker = Some(worker);
        }
    }

    fn _stop(&mut self) {
        if let Some(mut async_worker) = self.async_worker.take() {
            let ori_tasks = async_worker.acceptance();
            if let Some(ori_tasks) = ori_tasks {
                self.async_schedules.extend(ori_tasks);
            }
        }

        if let Some(mut worker) = self.worker.take() {
            let ori_tasks = worker.acceptance();
            if let Some(ori_tasks) = ori_tasks {
                self.schedules.extend(ori_tasks);
            }
        }
    }

    #[inline]
    fn _restart(&mut self, signal: Arc<AtomicBool>, tick_frequecy: Duration) {
        self._stop();
        self._start(signal, tick_frequecy);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::engine::sched::schedule::{Schedule, ScheduleAsync, ScheduleConf};

    use super::{Scheduler, Procedure};

    struct Task;

    impl Schedule for Task {
        fn token(&self) -> &'static str {
            "task"
        }

        fn nocked(&self, _: u64) -> bool {
            true
        }

        fn fire(&self, _: u64) {
            println!("what up?");
        }

        fn conf(&self) -> ScheduleConf {
            ScheduleConf::default()
        }
    }

    #[crate::async_trait]
    impl ScheduleAsync for Task {
        /// Identify of this schedule, e.g. it's name.
        fn token(&self) -> &'static str {
            "greeting"
        }

        fn conf(&self) -> ScheduleConf {
            ScheduleConf {
                ..Default::default()
            }
        }

        /// Notify scheduler prepare to fire, and check if should fire,
        /// if return true, then `fire`
        async fn nocked(&self, _: u64) -> bool {
            true
        }

        /// Schedule logic.
        async fn fire(&self, tick: u64) {
            println!("{tick} hello");
        }
    }

    #[test]
    fn test() {
        let mut procedure = Procedure::new();
        let _ = procedure.spawn_async(Task);
        let _ = procedure.spawn(Task);

        let scheduler = Scheduler::with_procedure(procedure, 1000);
        scheduler.start();

        std::thread::sleep(Duration::from_secs(5));
        scheduler.stop();

        scheduler.start();
        loop {}
    }
}
