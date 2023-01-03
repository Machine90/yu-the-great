use std::{sync::Arc, time::Duration};

use futures::Future;
use crate::vendor::prelude::*;

use crate::torrent::runtime;

use crate::{
    tokio::{task::JoinHandle, time::timeout},
    mailbox::api::{GroupMailBox, MailBox},
    RaftMsg,
    RaftMsgType::*,
    RaftResult,
};

pub struct Pipelines<R> {
    msgbox: Arc<dyn GroupMailBox>,
    inflights: Vec<(u64, JoinHandle<R>)>,
}

impl<R> Pipelines<R> {
    pub fn new(msgbox: Arc<dyn GroupMailBox>) -> Self {
        Self {
            msgbox,
            inflights: Vec::new(),
        }
    }

    /// Broadcast mails to target receiver in parallel async pipeline and get
    /// pipeline result by invoking
    /// [join](crate::mailbox::pipeline::Pipelines::join)
    /// to get pipeline result.
    /// ### Parameters
    /// * **mails**: attempt to broadcast to followers in group.
    /// * **response_handle**:
    ///   * *args*: (receiver_id, response)
    ///   * *return*: require for Future, e.g. ``` async { handle_response().await } ```
    /// ### Accept mail type
    /// * [MsgAppend](crate::RaftMsgType::MsgAppend)
    /// * [MsgSnapshot](crate::RaftMsgType::MsgSnapshot)
    /// * [MsgHeartbeat](crate::RaftMsgType::MsgHeartbeat)
    /// * [MsgRequestVote](crate::RaftMsgType::MsgRequestVote)
    /// * [MsgRequestPreVote](crate::RaftMsgType::MsgRequestPreVote)
    pub fn broadcast_with_async_cb<F, Fut>(&mut self, mails: Vec<RaftMsg>, response_handle: F)
    where
        F: Fn(u64, RaftResult<RaftMsg>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send,
        R: Send + 'static,
    {
        let response_handle = Arc::new(response_handle);
        for mail in mails {
            let to = mail.to;
            let resp_handler = response_handle.clone();
            let msgbox = self.msgbox.clone();
            let task = runtime::spawn(async move {
                resp_handler(to, Self::_dispatch_bcast_mail(mail, msgbox).await).await
            });
            self.inflights.push((to, task));
        }
    }

    pub fn broadcast_with_cb<F>(&mut self, mails: Vec<RaftMsg>, response_handle: F)
    where
        F: Fn(u64, RaftResult<RaftMsg>) -> R + Send + Sync + 'static,
        R: Send + Sync + 'static,
    {
        let response_handle = Arc::new(response_handle);
        for mail in mails {
            let to = mail.to;
            let resp_handler = response_handle.clone();
            let msgbox = self.msgbox.clone();
            let task = runtime::spawn(async move {
                let resp = Self::_dispatch_bcast_mail(mail, msgbox).await;
                resp_handler(to, resp)
            });
            self.inflights.push((to, task));
        }
    }

    async fn _dispatch_bcast_mail(
        mail: RaftMsg,
        msgbox: Arc<dyn GroupMailBox>,
    ) -> RaftResult<RaftMsg> {
        assert!(matches!(
            mail.msg_type(),
            MsgAppend | MsgSnapshot | MsgHeartbeat | MsgRequestVote | MsgRequestPreVote
        ));
        match mail.msg_type() {
            MsgAppend => msgbox.send_append(mail).await,
            MsgSnapshot => msgbox.send_snapshot(mail).await,
            MsgHeartbeat => msgbox.send_heartbeat(mail).await,
            MsgRequestVote | MsgRequestPreVote => msgbox.send_request_vote(mail).await,
            _ => unreachable!(),
        }
    }

    #[inline]
    pub async fn join(&mut self, permit: usize) -> Vec<(u64, R)> {
        self._join_collect(permit, None).await
    }

    #[inline]
    pub async fn join_timeout(&mut self, permit: usize, timeout: Duration) -> Vec<(u64, R)> {
        self._join_collect(permit, Some(timeout)).await
    }

    /// Join all tasks those spawned in `broadcast_with_async_cb` function until meet one response
    /// that matching the given `condition`. For example, we bcast 3 votes, and received 2 votes response
    /// satisfies the condition, thus we don't need to join remained 1.
    #[inline]
    pub async fn join_until(&mut self, condition: fn(response: &R) -> bool) -> Option<(u64, R)> {
        self._join_until(condition, None).await
    }

    #[inline]
    pub async fn join_until_in_timeout(
        &mut self,
        condition: fn(response: &R) -> bool,
        timeout: Duration,
    ) -> Option<(u64, R)> {
        self._join_until(condition, Some(timeout)).await
    }

    async fn _join_collect(
        &mut self,
        permit: usize,
        timeout_dur: Option<Duration>,
    ) -> Vec<(u64, R)> {
        let mut collected = vec![];
        let mut current = 0;

        for (peer, handle) in std::mem::take(&mut self.inflights) {
            if current >= permit {
                break;
            }
            let (acker, resp) = if let Some(timeout_dur) = timeout_dur {
                let timeout_resp = timeout(timeout_dur, handle).await;
                if let Err(e) = timeout_resp {
                    error!("timeout when join broadcast tasks, see: {:?}", e);
                    break;
                }
                (peer, timeout_resp.unwrap().unwrap())
            } else {
                let r = handle.await;
                (peer, r.unwrap())
            };
            collected.push((acker, resp));
            current += 1;
        }
        collected
    }

    async fn _join_until(
        &mut self,
        break_out: fn(&R) -> bool,
        timeout_dur: Option<Duration>,
    ) -> Option<(u64, R)> {
        for (peer, handle) in std::mem::take(&mut self.inflights) {
            let (acker, resp) = if let Some(timeout_dur) = timeout_dur {
                let timeout_resp = timeout(timeout_dur, handle).await;
                if let Err(e) = timeout_resp {
                    error!("timeout when join broadcast tasks, see: {:?}", e);
                    break;
                }
                (peer, timeout_resp.unwrap().unwrap())
            } else {
                let r = handle.await;
                (peer, r.unwrap())
            };
            if break_out(&resp) {
                return Some((acker, resp));
            }
        }
        None
    }
}
