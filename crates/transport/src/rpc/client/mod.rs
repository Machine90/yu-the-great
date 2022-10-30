pub mod group_client;

use std::{marker::PhantomData};
use common::protocol::{NodeID, GroupID};
use crate::{mailbox::{topo::{Topo}, LABEL_MAILBOX}};
use std::io::{Result};
use futures::Future;
use crate::vendor::prelude::*;
use crate::tokio;

use super::RaftServiceClient;
pub type RaftTransporter = Transporter<RaftServiceClient>;

#[derive(Clone)]
pub struct Transporter<C> {
    pub topo: Topo,
    pub _phan: PhantomData<C>,
}

impl<C: Send + Sync + Clone + 'static> Transporter<C> {

    pub fn from_topo(topo: Topo) -> Self {
        Self {
            topo,
            _phan: PhantomData
        }
    }

    /// Try acquire client of current (specified group and node) mailbox.
    pub async fn try_acquire_client(&self, group: GroupID, node: NodeID, reconnect: bool) -> Result<C> {
        let timeout_millis = self.topo.conf.connect_timeout_millis;
        let client = if reconnect {
            self.topo.reconnect_and_get(&group, &node, LABEL_MAILBOX, timeout_millis).await
        } else {
            self.topo.get_client_timeout(&group, &node, LABEL_MAILBOX, timeout_millis).await
        };
        if let Err(ref e) = &client {
            warn!("failed to connect to group-{group} node-{node}, detail: {:?}", e);
        }
        client
    }

    pub async fn try_acquire_node_client(&self, node: NodeID, reconnect: bool) -> Result<C> {
        let timeout_millis = self.topo.conf.connect_timeout_millis;
        let client = if reconnect {
            self.topo.reconnect_and_get_node(&node, LABEL_MAILBOX, timeout_millis).await
        } else {
            self.topo.get_node_client_timeout(&node, LABEL_MAILBOX, timeout_millis).await
        };
        if let Err(ref e) = &client {
            warn!("failed to connect to node-{node}, detail: {:?}", e);
        }
        client
    }

    /// Make a retryable RPC call to "current" node (specific group and node). 
    /// this will retry if connection refused.
    /// ### Example
    /// ```rust
    /// let name: String = "Alice".to_owned();
    /// transporter.make_call_to_peer(1, 1, move |service| {
    ///     let to_who = name.clone();
    ///     async move {
    ///         let greeting: String = service.say_hello(to_who).await;
    ///         Ok(greeting)
    ///     }
    /// });
    /// ```
    pub async fn make_call_to_peer<H, Fut, R>(&self, group: GroupID, node: NodeID, mut handle: H) -> Result<R> 
    where 
        H: FnMut(C) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + Sync,
        R: Send + Sync + 'static,
    {
        let client = self.try_acquire_client(group, node, false).await?;
        let conn_timeout = self.topo.conf.connect_timeout();
        // TODO: more graceful to retry, e.g. with policy
        let tried = tokio::time::timeout(conn_timeout, handle(client));
        let invoked = tried.await;
        let result = if let Err(e) = invoked {
            warn!(
                "timeout when making RPC call to group-{group} node-{node}, attempt to retry, see: {:?}",
                e
            );
            let client = self.try_acquire_client(group, node, true).await?;
            let tried = tokio::time::timeout(conn_timeout, handle(client));
            let invoked = tried.await;
            invoked?
        } else {
            invoked.unwrap()
        };
        result
    }

    pub async fn make_call_to_node<H, Fut, R>(&self, node: NodeID, mut handle: H) -> Result<R> 
    where 
        H: FnMut(C) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + Sync,
        R: Send + Sync + 'static,
    {
        let client = self.try_acquire_node_client(node, false).await?;
        let conn_timeout = self.topo.conf.connect_timeout();
        
        let tried = tokio::time::timeout(conn_timeout, handle(client));
        let invoked = tried.await;
        let result = if let Err(e) = invoked {
            warn!(
                "timeout when making RPC call to node-{node}, attempt to retry, see: {:?}",
                e
            );
            let client = self.try_acquire_node_client(node, true).await?;
            let tried = tokio::time::timeout(conn_timeout, handle(client));
            let invoked = tried.await;
            invoked?
        } else {
            invoked.unwrap()
        };
        result
    }
}
