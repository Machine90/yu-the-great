use std::{
    hint::spin_loop,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
    thread, collections::HashMap,
};

use components::torrent::runtime;

use super::schedule::{RaftScheduleAsync, RaftSchedule, ScheduleConf};
use crate::{error,tokio::{self, task, time::{Instant, timeout}}};

pub(super) struct Worker {
    scheduling: Option<thread::JoinHandle<HashMap<&'static str, RaftSchedule>>>,
    pub offered: usize,
    pub start_time: Instant,
}

impl Default for Worker {
    fn default() -> Self {
        Self { 
            scheduling: None, 
            offered: 0, 
            start_time: Instant::now(),
        }
    }
}

impl Worker {

    pub fn offer(
        &mut self,
        running: Arc<AtomicBool>,
        interval: Duration,
        schedules: HashMap<&'static str, RaftSchedule>,
    ) {
        let offered = schedules.len();
        let scheduling = thread::spawn(move || {
            let mut count = 0;
            while running.load(Ordering::Relaxed) {
                std::thread::sleep(interval);
                let mut tasks = Vec::new();
                for (_, schedule) in schedules.iter() {
                    if !schedule.nocked(count) {
                        continue;
                    }
                    let ScheduleConf { should_parallel, should_join, .. } = schedule.conf();
                    
                    if should_parallel {
                        let schedule = schedule.clone();
                        let task = thread::spawn(move || {
                            schedule.fire(count);
                        });
                        if should_join {
                            tasks.push(task);
                        }
                    } else {
                        schedule.fire(count);
                    }
                }
                if !tasks.is_empty() {
                    for task in tasks {
                        // TODO: join in timeout
                        let _r = task.join();
                    }
                }
                count += 1;
                spin_loop();
            }
            schedules
        });
        self.start_time = Instant::now();
        self.offered = offered;
        self.scheduling = Some(scheduling);
    }

    pub fn acceptance(&mut self) -> Option<HashMap<&'static str, RaftSchedule>> {
        let scheduling = self.scheduling.take()?;
        let schedules = scheduling.join();
        if let Err(e) = &schedules {
            error!("failed to joined async scheduling, see: {:?}", e);
        }
        schedules.ok()
    }
}


pub(super) struct AsyncWorker {
    scheduling: Option<task::JoinHandle<HashMap<&'static str, RaftScheduleAsync>>>,
    pub offered: usize,
    pub start_time: Instant,
}

impl Default for AsyncWorker {
    fn default() -> Self {
        Self { 
            scheduling: None, 
            offered: 0, 
            start_time: Instant::now(),
        }
    }
}

impl AsyncWorker {

    pub fn offer(
        &mut self,
        running: Arc<AtomicBool>,
        interval: Duration,
        schedules: HashMap<&'static str, RaftScheduleAsync>
    ) {
        let offered = schedules.len();
        
        let scheduling = runtime::spawn(async move {
            let mut count = 0;
            while running.load(Ordering::Relaxed) {
                spin_loop();
                tokio::time::sleep(interval).await;
                let mut tasks = Vec::new();
                for (_, schedule) in schedules.iter() {
                    let conf = schedule.conf();
                    let wait_timeout = conf.scheduling_timeout;
                    let should_join = conf.should_join;

                    let nock_timeout = timeout(
                        wait_timeout, 
                        schedule.nocked(count)
                    );
                    let nocked = nock_timeout.await;
                    if nocked.is_err() {
                        continue;
                    }
                    if !nocked.unwrap() {
                        continue;
                    }

                    if conf.should_parallel {
                        let schedule = schedule.clone();
                        let schedule_timeout = timeout(
                            wait_timeout, 
                            async move { schedule.fire(count).await }
                        );
                        let task = runtime::spawn(async move {
                            let _ = schedule_timeout.await;
                        });
                        if should_join {
                            tasks.push(task);
                        }
                    } else {
                        let schedule_timeout = timeout(
                            wait_timeout, 
                            schedule.fire(count)
                        );
                        let _ = schedule_timeout.await;
                    }
                }

                if !tasks.is_empty() {
                    for task in tasks {
                        // TODO: join in timeout
                        let _r = task.await;
                    }
                }
                count += 1;
            }
            schedules
        });
        self.start_time = Instant::now();
        self.offered = offered;
        self.scheduling = Some(scheduling);
    }

    pub fn acceptance(&mut self) -> Option<HashMap<&'static str, RaftScheduleAsync>> {
        let scheduling = self.scheduling.take()?;
        let schedules = runtime::blocking(async move {
            scheduling.await
        });
        if let Err(e) = &schedules {
            error!("failed to joined async scheduling, see: {:?}", e);
        }
        schedules.ok()
    }
}
