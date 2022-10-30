use std::{time::Duration};
use torrent::{tokio1::{sync::broadcast::Receiver, time::timeout}, Future, runtime};
use super::{_timeout_err, _join_err, _recv_err};
use common::errors::{Result as RaftResult, Error};

#[derive(Default)]
pub struct PendingRecv<R> {
    pub(super) receiver: Option<Receiver<R>>
}

impl<R: Clone> PendingRecv<R> {

    pub fn new(receiver: Receiver<R>) -> Self {
        Self { receiver: Some(receiver) }
    }

    pub async fn get(mut self, wait_timeout: Duration) -> RaftResult<R> {
        let mut responder = self.receiver.take()
            .ok_or(Error::Nothing)?;

        let responder = timeout(
            wait_timeout,
            responder.recv(),
        );
        let chain = responder.await;
        if let Err(_) = chain {
            return Err(_timeout_err(wait_timeout));
        }
        let chain = chain.unwrap();
        if let Err(_) = chain {
            return Err(_recv_err());
        }
        Ok(chain.unwrap())
    }

    /// Handle the process in a standalone async task, then blocking for the 
    /// result in timeout.
    pub async fn before<P>(self, process: P, wait_timeout: Duration) -> RaftResult<R>
    where 
        P: Future<Output = ()> + Send + 'static
    {
        let _do_process = runtime::spawn(process);
        self.get(wait_timeout).await
    }
}


impl<R: Clone + Send + Sync + 'static> PendingRecv<R> {

    /// Spawn a new async task for receiving result, then handle process in
    /// current thread. Then acquire the result from async task after advanced
    /// process.
    pub async fn after<P>(mut self, process: P, wait_timeout: Duration) -> RaftResult<R> 
    where 
        P: Future<Output = ()>
    {
        let mut responder = self.receiver.take()
            .ok_or(Error::Nothing)?;

        let future = runtime::spawn(async move {
            let in_timeout = timeout(
                wait_timeout,
                responder.recv(),
            );

            let chain = in_timeout.await;
            if let Err(_) = chain {
                return Err(_timeout_err(wait_timeout));
            }
            let chain = chain.unwrap();
            if let Err(_) = chain {
                return Err(_recv_err());
            }
            Ok(chain.unwrap())
        });

        process.await;
        let result = future.await;
        if let Err(e) = result {
            return Err(_join_err(e));
        }
        result.unwrap()
    }
}
