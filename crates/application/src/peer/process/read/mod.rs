pub mod follower;

use std::fmt::{Debug, Display};
use common::protocol::{response::Response, read_state::ReadState};
use consensus::{
    prelude::{raft_role::RaftRole},
    raft_node::{raft_functions::RaftFunctions, raft_process::RaftProcess, Ready},
};
use crate::{
    coprocessor::{listener::RaftContext, read_index_ctx::{ReadContext, MaybeReady}},
    mailbox::api::MailBox,
    peer::{
        process::{at_most_one_msg, msgs},
        raft_group::raft_node::WLockRaft,
        Peer,
    },
    utils::drain_filter,
    ConsensusError::{self},
    RaftMsg,
    RaftMsgType::{MsgReadIndex, MsgReadIndexResp},
    RaftResult,
};

pub type ReadedState = ReadState;

/// Ready for reading.
#[derive(Default, Debug)]
pub struct ReadyRead {
    pub responses: Vec<RaftMsg>,
    pub read_states: Vec<ReadedState>,
}

impl Display for ReadyRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let resp_cnt = self.responses.len();
        let rss_cnt = self.read_states.len();
        write!(
            f,
            "total {resp_cnt} `forward reads` and {rss_cnt} `read_states`"
        )
    }
}

impl ReadyRead {
    pub fn resps(responses: Vec<RaftMsg>) -> Self {
        Self {
            responses,
            read_states: Vec::new(),
        }
    }

    pub fn rss(rss: Vec<ReadedState>) -> Self {
        Self {
            responses: Vec::new(),
            read_states: rss,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.responses.is_empty() && self.read_states.is_empty()
    }
}

enum RouteRead {
    /// Group leader recv read request and must ensure it's
    /// still leader before do really read (in safe read mode).
    GuaranteeRead(Vec<RaftMsg>),
    /// standalone or leasebased read will read local directly,
    /// then generate some rss.
    LocalRead(Vec<ReadState>),
    ResponseForward(RaftMsg),
    Forward(RaftMsg),
    Drop(String),
}

impl Debug for RouteRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GuaranteeRead(hb) => {
                write!(f, "GuaranteeRead with {:?} heartbeat msgs", hb.len())
            }
            Self::LocalRead(_) => f.debug_tuple("LocalRead").finish(),
            Self::ResponseForward(_) => f.debug_tuple("ResponseForward").finish(),
            Self::Forward(_) => f.debug_tuple("Forward").finish(),
            Self::Drop(arg0) => f.debug_tuple("Drop").field(arg0).finish(),
        }
    }
}

impl Peer {
    /// `Leader` receive forwarded read_index message from follower and response to this
    /// forwarder after handle it.
    /// When read_only in [Safe](consensus::raft::read_only::ReadOnlyOption::Safe) mode,
    /// this leader forced to bcast heartbeat with read_index ctx to ensure it's still leader.
    /// Otherwise return the response to forwarder directly without any RPC transport and storage query.
    pub async fn recv_forward_read(
        &self, 
        read_index: RaftMsg, 
        mut forward_read_ctx: ReadContext
    ) -> RaftResult<RaftMsg> {
        assert_eq!(
            read_index.msg_type(),
            MsgReadIndex,
            "only accept MsgReadIndex"
        );

        let mut raft = self.wl_raft().await;
        if raft.role() != RaftRole::Leader && self.conf().allow_forward_spread {
            // maybe this peer is old leader, and became follower before received this read_index.
            return Err(ConsensusError::ProposalDropped(
                "I'm not Leader anymore, disallow to forward read request more than twice".into(),
            ));
        }

        // otherwise handle it as Leader.
        let ready = raft.step_and_ready(read_index)?;

        let (_, next) = self._route_read(raft, ready).await;

        match next {
            RouteRead::Forward(read_index) => {
                // when recv read_index as follower, then forward this read to leader.
                let keep_forward = self.mailbox.redirect_read_index(read_index).await?;
                Ok(keep_forward)
            }
            RouteRead::ResponseForward(resp) => Ok(resp),
            // when read_only was set to Safe, and this read_index received from forwarded.
            RouteRead::GuaranteeRead(hb_with_read) => {
                self._handle_guarantee_read(hb_with_read, &mut forward_read_ctx).await?;
                let result = forward_read_ctx
                    .take_response()
                    .ok_or(ConsensusError::NotReachQuorum)?;
                result.into()
            }
            RouteRead::Drop(reason) => Err(ConsensusError::ProposalDropped(reason)),
            RouteRead::LocalRead(_) => Err(ConsensusError::Other(
                "stepped in unexpected `StandaloneRead` case".into(),
            )),
        }
    }

    /// `Leader` or `Follower` recv read request then decide to process this read (as leader)
    /// or forward to leader (as follower).
    pub async fn handle_or_forward_read(&self, mut read_ctx: ReadContext) -> RaftResult<ReadedState> {
        let mut raft = self.wl_raft().await;
        raft.read_index(read_ctx.interested_read().unwrap().clone());
        if !raft.has_ready() {
            return Err(ConsensusError::ProposalDropped(format!(
                "maybe no leader now, proposal dropped, please check the log of Node {:?}",
                self.node_id()
            )));
        }
        let ready = raft.get_ready();
        let (ctx, next) = self._route_read(raft, ready).await;

        
        match next {
            RouteRead::Forward(read_index) => {
                // when recv read as follower, then forward this read to leader.
                let readindex_resp = self.mailbox
                    .redirect_read_index(read_index)
                    .await;

                if let Err(err) = readindex_resp {
                    // TODO: because `Error` is unable to clone, so convert to 
                    // `Response` first, have any graceful approach?
                    let error: Response<()> = (&err).into();
                    self._advance_read_error(read_ctx, error.into()).await?;
                    return Err(err);
                }

                let readed_state = self
                    .handle_read_index_resp(read_ctx, readindex_resp.unwrap())
                    .await?;
                Ok(readed_state)
            }
            RouteRead::LocalRead(rss) => {
                // handle read_index as standalone leader.
                self.coprocessor_driver
                    .advance_read(&ctx, read_ctx.with_ready(ReadyRead::rss(rss)))
                    .await?;
                read_ctx
                    .take_readed()
                    .ok_or(ConsensusError::ProposalDropped(
                        "failed to `read_index` from local".to_owned(),
                    ))?.into()
            }
            RouteRead::GuaranteeRead(hb_with_read) => {
                // handle read_index that request from Leader itself and in safe mode.
                self._handle_guarantee_read(hb_with_read, &mut read_ctx).await?;
                read_ctx
                    .take_readed()
                    .ok_or(ConsensusError::ProposalDropped(
                        "failed to `read_index` after heartbeat".to_owned(),
                    ))?.into()
            }
            RouteRead::Drop(reason) => Err(ConsensusError::ProposalDropped(reason)),
            _ => {
                Err(ConsensusError::Other(
                    format!("node-{:?} stepped in unexpected case", self.node_id()).into(),
                ))
            },
        }
    }

    async fn _advance_read_error(
        &self,
        mut read_request_ctx: ReadContext,
        err: ConsensusError
    ) -> RaftResult<()> {
        let error = MaybeReady::Error(err);
        read_request_ctx.maybe_ready(error);
        let ctx = self.get_context(false).await;
        self.coprocessor_driver
            .advance_read(&ctx, &mut read_request_ctx)
            .await?;
        Ok(())
    }

    async fn _handle_guarantee_read(
        &self, 
        heartbeat: Vec<RaftMsg>, 
        read_request_ctx: &mut ReadContext,
    ) -> RaftResult<()> {
        let ready = self._bcast_read(heartbeat).await;
        read_request_ctx.maybe_ready(ready);
        let ctx = self.get_context(false).await;
        self.coprocessor_driver
            .advance_read(&ctx, read_request_ctx)
            .await?;
        Ok(())
    }

    async fn _bcast_read(
        &self, 
        heartbeat: Vec<RaftMsg>, 
    ) -> MaybeReady {
        // handle read_index that request from Leader itself and in safe mode.
        let hb_result = self.broadcast_heartbeat(heartbeat, true).await;
        if let Err(e) = hb_result {
            crate::error!("bcast `read_index` error: {:?}", e);
            return MaybeReady::Error(e);
        }

        let (_, _, readed) = hb_result.unwrap();

        if let Some(readed) = readed {
            MaybeReady::Succeed(readed)
        } else {
            // the read request maybe advanced by other read or heartbeat.
            MaybeReady::NotReady
        }
    }

    async fn _route_read(
        &self,
        mut raft: WLockRaft<'_>,
        mut ready: Ready,
    ) -> (RaftContext, RouteRead) {
        let current_role = raft.role();
        // if leader, maybe send broadcast to ensure if still leader.
        // or leader may response for follower forward read_index.
        let mut heartbeat_or_read_response = msgs(ready.take_messages());

        let rss = if !ready.get_read_states().is_empty() {
            // while standalone leader handle read_index.
            Some(ready.take_read_states())
        } else {
            None
        };

        let mut light_rd = raft.advance(ready);
        // follower: generate forward read to leader.
        let forward_read = at_most_one_msg(light_rd.take_messages());

        let raft_ctx: RaftContext = RaftContext::from_status(self.get_group_id(), raft.status());
        drop(raft);

        let next = if current_role == RaftRole::Leader {
            // when leader process read_index, some broadcast will be generated if in ReadIndexOption Safe mode.
            if !heartbeat_or_read_response.is_empty() {
                // there must be at least 1 read resp in theory.
                let mut resp = drain_filter(
                    &mut heartbeat_or_read_response,
                    |msg| msg.msg_type() == MsgReadIndexResp,
                    |resp| Some(resp),
                );
                if !resp.is_empty() {
                    RouteRead::ResponseForward(resp.pop().unwrap())
                } else {
                    RouteRead::GuaranteeRead(heartbeat_or_read_response)
                }
            } else if rss.is_some() {
                RouteRead::LocalRead(rss.unwrap())
            } else {
                RouteRead::Drop(format!(
                    "committed {:?} is not matched current term: {:?}",
                    raft_ctx.commit, raft_ctx.term
                ))
            }
        } else {
            if let Some(forward) = forward_read {
                RouteRead::Forward(forward)
            } else {
                RouteRead::Drop(format!("not leader at term: {:?}", raft_ctx.term))
            }
        };
        (raft_ctx, next)
    }
}
