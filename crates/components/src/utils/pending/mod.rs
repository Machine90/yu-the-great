pub mod receiver;

use std::{hash::Hash, ops::Deref, sync::Arc, time::Duration, io::{Error, ErrorKind}};

use crate::tokio1::{
    sync::broadcast::{channel, Sender},
    task::{JoinError}
};
use common::errors::{Error as ConsensusError, Result as RaftResult};

use crate::{
    vendor::prelude::{mapref::entry, DashMap}
};
use torrent::futures::Future;

use self::receiver::PendingRecv;

/// Same as "Event bus", "Pending" allow to register topic to it, 
/// then others can subscribe interested topic if exists, the first
/// publisher should provide an "advancer" which used to consume topic,
/// once the topic has been consumed, consumer should take out the 
/// notifier of topic and bcast "result" to each subscribers.
pub struct Pending<I, R> {
    topics: Arc<Topics<I, R>>,
}

impl<I: Hash + PartialEq + Eq, R> Default for Pending<I, R> {
    fn default() -> Self {
        Self { topics: Arc::new(Topics::default()) }
    }
}

impl<I: Hash + PartialEq + Eq, R: Clone> Pending<I, R> {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(Topics::new()),
        }
    }
}

impl<I, R> Deref for Pending<I, R> {
    type Target = Topics<I, R>;

    fn deref(&self) -> &Self::Target {
        self.topics.as_ref()
    }
}

pub struct Topics<I, R> {
    table: DashMap<I, Sender<R>>,
}

impl<I: Hash + PartialEq + Eq, R> Default for Topics<I, R> {
    fn default() -> Self {
        Self { table: Default::default() }
    }
}

impl<I: Hash + PartialEq + Eq, R: Clone> Topics<I, R> {
    pub fn new() -> Self {
        Self {
            table: DashMap::new(),
        }
    }

    /// Once a topic and it's notifier has been take out, then this topic can't
    /// subscribe anymore.
    #[inline]
    pub fn take_notifier(&self, interested: &I) -> Option<Sender<R>> {
        self.table.remove(interested).map(|(_, notifier)| notifier)
    }
}

impl<I: Clone + Hash + PartialEq + Eq, R: Clone + Send + Sync + 'static> Pending<I, R> {

    pub fn sub_or_pub(&self, interested: I, cap: usize) -> (PendingRecv<R>, bool) {
        match self.topics.table.entry(interested) {
            entry::Entry::Occupied(ent) => {
                (PendingRecv::new(ent.get().subscribe()) , false)
            },
            entry::Entry::Vacant(ent) => {
                let (tx, receiver) = channel(cap);
                ent.insert(tx);
                (PendingRecv::new(receiver), true)
            }
        }
    }

    pub fn pub_if_absent(&self, interested: I, cap: usize) -> Option<PendingRecv<R>> {
        match self.topics.table.entry(interested) {
            entry::Entry::Vacant(ent) => {
                let (tx, receiver) = channel(cap);
                ent.insert(tx);
                let receiver = PendingRecv::new(receiver);
                Some(receiver)
            }
            _ => None
        }
    }

    #[inline]
    async fn _try_subscribe(&self, interested: &I) -> Option<PendingRecv<R>> {
        self.topics.table.get(interested).map(|tx| 
            PendingRecv::new(tx.subscribe()) 
        )
    }

    /// Try waiting advanced result of interested topoic in timeout.
    /// Return None if interested not registered.
    pub async fn try_subscribe(&self, interested: &I, wait_timeout: Duration) -> Option<RaftResult<R>> {
        let responder = self._try_subscribe(interested).await?;
        let result = responder.get(wait_timeout).await;
        Some(result)
    }

    pub async fn try_advance<Fut>(
        &self,
        interested: &I,
        process: Fut,
        wait_timeout: Duration,
    ) -> RaftResult<R> 
    where
        Fut: Future<Output = ()>
    {
        if let Some(responder) = self._try_subscribe(interested).await {
            responder.after(process, wait_timeout).await
        } else {
            return Err(_topic_absent());
        }
    }

    /// Try to publish a topic, if the interested topic existed, 
    /// then just waiting for result in timeout, otherwise invoking
    /// "process" to handle this topic.
    pub async fn observe<Fut>(
        &self,
        interested: &I,
        process: Fut,
        max_receiver: usize,
        wait_timeout: Duration,
    ) -> RaftResult<R>
    where
        Fut: Future<Output = ()>
    {
        let (responder, new_topic) = self.sub_or_pub(
            interested.clone(), 
            max_receiver
        );
        
        if new_topic {
            responder.after(process, wait_timeout).await
        } else {
            responder.get(wait_timeout).await
        }
    }
}

#[inline]
pub(self) fn _timeout_err(dur: Duration) -> ConsensusError {
    ConsensusError::Io(Error::new(
        ErrorKind::TimedOut, 
        format!("couldn't to observe topic in {:?}ms", dur.as_millis())
    ))
}

#[inline]
pub(self) fn _join_err(e: JoinError) -> ConsensusError {
    ConsensusError::Io(Error::new(
        ErrorKind::Interrupted, 
        format!("failed to join task of observing topic, detail: {:?}", e)
    ))
}

#[inline]
pub(self) fn _recv_err() -> ConsensusError {
    ConsensusError::Io(Error::new(
        ErrorKind::Other, 
        format!("failed to receive result, maybe notifier is dropped")
    ))
}

#[inline]
pub(self) fn _topic_absent() -> ConsensusError {
    ConsensusError::Io(Error::new(
        ErrorKind::NotFound, 
        format!("interested topic not found")
    ))
}

#[cfg(test)] mod tests {
    use std::{sync::Arc};
    use torrent::runtime;
    use super::*;

    #[test] fn test() {
        let r = Arc::new(Pending::<i32, i32>::default());
        let mut task = vec![];
        for i in 0..10 {
            let reader = r.clone();
            task.push(runtime::spawn(async move {
                let mut cnt = 0;
                for j in 0..10 {
                    let topic = j % 2;
                    let res = reader.observe(
                        &topic, 
                        async {
                            let interested = topic;
                            let topics = reader.topics.clone();
                            let notifier = topics.take_notifier(&interested);
                            if let Some(n) = notifier {
                                let _ = n.send(i);
                            }
                        }, 
                        10, 
                        Duration::from_millis(100)
                    ).await;
                    if res.is_ok() {
                        cnt += 1;
                    }
                    println!("{i} => {:?}", res);
                }
                println!("thread-{i} cnt: {cnt}");
            }));
        }

        runtime::blocking(async move {
            for t in task {
                let _ = t.await;
            }
        });
    }
}