use std::{
    convert::{TryInto},
    time::Duration,
};

use crate::{
    mailbox::api::MailBox,
    peer::{
        process::msgs, raft_group::raft_node::WLockRaft, Peer,
    },
    ConsensusError, RaftMsg,
    RaftMsgType::MsgPropose,
    RaftResult, 
    coprocessor::listener::RaftContext,
};
use common::protocol::proposal::Proposal;
use consensus::{
    prelude::raft_role::RaftRole,
    raft_node::{raft_functions::RaftFunctions, raft_process::RaftProcess, Ready},
};
use crate::protos::prelude::BatchConfChange;
use crate::vendor::prelude::*;

enum RouteProposal {
    /// Leader found some broadcast append after step propose.
    Broadcast(Vec<RaftMsg>),
    /// Follower attempt to redirect propose to leader.
    Redirect(RaftMsg),
    /// Has some commit as single leader.
    Commit(u64, Option<Vec<RaftMsg>>),
    /// Neither append nor commit, it's a issue?
    Pending(RaftContext),
}

impl Peer {
    /// Receive propose as Leader or Follower. If follower receive proposal, then redirect
    /// it to Leader, otherwise handle it and get result.
    pub async fn handle_or_forward_propose(
        &self,
        propose: Vec<u8>,
        timeout_dur: Duration,
    ) -> RaftResult<Proposal> {
        let mut raft = self.raft_group.wl_raft().await;
        raft.propose(propose)?;
        let ready = raft.get_if_ready()?;
        self._advance_propose_ready(raft, ready, timeout_dur).await
    }

    pub async fn handle_or_forward_conf_changes(
        &self, 
        changes: BatchConfChange,
        timeout_dur: Duration,
    ) -> RaftResult<Proposal> {
        let mut raft = self.wl_raft().await;
        raft.propose_conf_change(changes)?;
        let ready = raft.get_if_ready()?;
        self._advance_propose_ready(raft, ready, timeout_dur).await
    }

    /// Receive propose as Leader or Follower. If follower receive proposal, then redirect
    /// it to Leader, otherwise handle it and get result.
    pub async fn handle_or_forward_propose_cmd(
        &self,
        cmd: Vec<u8>,
        timeout_dur: Duration,
    ) -> RaftResult<Proposal> {
        let mut raft = self.wl_raft().await;
        raft.propose_cmd(cmd)?;
        let ready = raft.get_if_ready()?;
        self._advance_propose_ready(raft, ready, timeout_dur).await
    }

    /// If leader received propose (from follower), then consume it.
    /// otherwise if follower (maybe old leader) received propose,  
    /// determine if allow to continue redirect to new leader.
    pub async fn recv_forward_propose(&self, propose: RaftMsg) -> RaftResult<Proposal> {
        assert_eq!(propose.msg_type(), MsgPropose);
        
        let mut raft = self.raft_group.wl_raft().await;
        let role = raft.role();
        if role != RaftRole::Leader && !self.conf().allow_forward_spread {
            warn!("receive proposal as {:?} while disallow proposal propagation.", role);
            return Err(ConsensusError::ProposalDropped("disallow forward propose more than twice".into()));
        }
        let ready = raft.step_and_ready(propose)?;
        self._advance_propose_ready(raft, ready, self.conf().max_wait_append_duration()).await
    }

    async fn _advance_propose_ready(
        &self, 
        raft: WLockRaft<'_>,
        ready: Ready,
        timeout_dur: Duration,
    ) -> RaftResult<Proposal> {
        return match self._route_proposal(raft, ready).await? {
            RouteProposal::Broadcast(append) => self.broadcast_append(append, timeout_dur).await,
            RouteProposal::Redirect(propose) => {
                // just send propose to leader when current node is follower.
                // and waiting for result.
                self.mailbox.redirect_proposal(propose).await.try_into()
            }
            RouteProposal::Commit(idx, bcast_commit) => {
                // when single leader without any followers.
                if let Some(bcast_commit) = bcast_commit {
                    // if let some commit msg need to broadcast.
                    self.broadcast_commit(bcast_commit).await;
                }
                Ok(Proposal::Commit(idx))
            }
            RouteProposal::Pending(ctx) => {
                warn!("nothing to commit after step proposal: {:?}", ctx);
                Ok(Proposal::Pending)
            }
        };
    }

    async fn _route_proposal(
        &self,
        mut raft: WLockRaft<'_>,
        mut ready: Ready,
    ) -> RaftResult<RouteProposal> {
        let commit_proposal = self.persist_ready(&mut ready).await?;
        let appends = msgs(ready.take_messages());
        let role = raft.role();
        let mut light_ready = raft.advance(ready);
        
        // maybe generate some committed entry after advance ready after recv propose.
        // e.g. single node group, leader commit immediately without compute quorum's commit
        // after recv response from appends.
        let commit_standalone = self.apply_commit_entry(&mut raft, light_ready.take_committed_entries());
        let redirect_or_commit = msgs(light_ready.take_messages());
        raft.advance_apply();
        let group = self.get_group_id();
        let ctx: RaftContext = RaftContext::from_status(group, raft.status());
        drop(raft);

        commit_standalone.join_all().await;

        // maybe update commit
        let commit = self.persist_light_ready(&mut light_ready).await?;
        let commit = commit.or(commit_proposal);
        let next_step = if !appends.is_empty() && role == RaftRole::Leader {
            // if leader has some append to broadcast.
            RouteProposal::Broadcast(appends)
        } else if !redirect_or_commit.is_empty() {
            // if role is follower then just redirect propose to leader.
            // or role is single leader (without any follower to append), then just commit.
            if role == RaftRole::Follower {
                // redirect propose as follower.
                RouteProposal::Redirect(redirect_or_commit[0].to_owned())
            } else if commit.is_some() {
                // broadcast commit
                RouteProposal::Commit(commit.unwrap(), Some(redirect_or_commit))
            } else {
                // not commit as leader
                RouteProposal::Pending(ctx)
            }
        } else {
            // not append and not redirect, always handle propose as standalone leader.
            if let Some(commit) = commit {
                RouteProposal::Commit(commit, None)
            } else {
                RouteProposal::Pending(ctx)
            }
        };
        Ok(next_step)
    }
}
