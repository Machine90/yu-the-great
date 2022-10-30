use std::collections::HashSet;
use std::fmt::Debug;

use components::mailbox::topo::PeerID;
use consensus::prelude::*;
use crate::coprocessor::read_index_ctx::ReadContext;
use crate::vendor::prelude::*;

use crate::peer::{Peer, Core, process::msgs, raft_group::raft_node::WLockRaft};
use crate::{RaftMsg, RaftResult};

use super::ChangeReason;

#[derive(Debug)]
pub enum Kinds {
    Election,
    Heartbeat(HashSet<PeerID>),
    PrepareHeartbeat(Vec<RaftMsg>),
    PrepareElection(Vec<RaftMsg>),
    /// When in this case, always means:
    /// * Elapsed has not reached limit, neither heartbeat interval nor election.
    /// * If standalone leader trigger to heartbeat
    Nothing,
}

/// Tick result
#[allow(unused)]
#[derive(Debug)]
pub struct Ticked {
    /// final role after ticked
    pub role: RaftRole,
    pub kind: Kinds,
}

enum NextState {
    Election(Vec<RaftMsg>),
    Heartbeat(Vec<RaftMsg>),
    // Peer is not leader anymore, majority voters were disappeared
    NotQuorum, Finished,
}

impl Debug for NextState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Election(followers) => {
                let to: Vec<_> = followers.iter().map(|m| m.to).collect();
                write!(f, "Election to: {:?}", to)
            },
            Self::Heartbeat(followers) => {
                let to: Vec<_> = followers.iter().map(|m| m.to).collect();
                write!(f, "Heartbeat to: {:?}", to)
            },
            Self::NotQuorum => write!(f, "NotQuorum"),
            Self::Finished => write!(f, "Finished"),
        }
    }
}

impl Peer {

    /// Try to tick current raft node, and maybe bcast heartbeats as 
    /// Leader or request election as Follower, and all broadcast msg
    /// (heartbeat / votes) will be sent if do it immediately.
    pub async fn tick(&self, do_heartbeat: bool, do_election: bool) -> RaftResult<Ticked> {
        let mut tick_raft = self.wl_raft().await;
        let ticked = tick_raft.tick();
        if !ticked || !tick_raft.has_ready() {
            return Ok(Ticked { kind: Kinds::Nothing, role: tick_raft.role() });
        }
        let Status { soft_state, .. } = tick_raft.status();
        
        let ready = tick_raft.get_ready();
        let (
            role, next_state
        ) = self._next_tick_step(tick_raft, ready, &soft_state).await;
        let (new_role, action, kind) = match next_state {
            NextState::Election(votes) => {
                if !do_election {
                    (role, "prepare election", Kinds::PrepareElection(votes))
                } else {
                    let role = self.broadcast_vote(role, votes).await?;
                    (role, "election", Kinds::Election)
                }
            },
            // only collect role from heartbeat when invoke at tick.
            NextState::Heartbeat(heartbeats) => {
                if !do_heartbeat {
                    (role, "prepare heartbeat", Kinds::PrepareHeartbeat(heartbeats))
                } else {
                    let (
                        ticked_role, 
                        no_acks,
                        ready_to_read,
                    ) = self.broadcast_heartbeat(heartbeats, false).await?;

                    let ctx = self.get_context(false).await;
                    if let Some(ready_to_read) = ready_to_read {
                        // advance ready read
                        if let Err(e) = self.coprocessor_driver.advance_read(
                            &ctx,
                            ReadContext::from_hb().with_ready(ready_to_read)
                        ).await {
                            warn!("failed to advance read request when tick, see: {:?}", e);
                        }
                    }
                    (ticked_role, "heartbeat", Kinds::Heartbeat(no_acks))
                }
            },
            NextState::NotQuorum => (role, "check quorum", Kinds::Heartbeat(HashSet::default())),
            NextState::Finished => (role, "standalone election", Kinds::Election),
        };
        if role != new_role {
            debug!(
                "group-{:?} role transfered after {:?}? origin role: {:?} new role: {:?}", 
                self.get_group_id(), action, role, new_role
            );
        }
        Ok(Ticked { kind, role: new_role })
    }
}

impl Core {

    async fn _next_tick_step(&self, mut raft: WLockRaft<'_>, mut ready: Ready, prev_ss: &SoftState) -> (RaftRole, NextState) {
        let mut next = if ready.get_messages().is_empty() {
            None
        } else {
            let heartbeat = msgs(ready.take_messages());
            Some(NextState::Heartbeat(heartbeat))
        };

        if let Some(ss) = ready.soft_state_ref() {
            // role has been changed, Leader => Follower or Follwer => (Pre)Candidate.
            self.on_soft_state_change(prev_ss, ss, ChangeReason::Tick).await;
        }
        let mut light_rd = raft.advance(ready);
        if !light_rd.get_messages().is_empty() {
            assert!(next.is_none(), "A peer could never both broadcast heartbeat and votes at same time!");
            let votes = msgs(light_rd.take_messages());
            next = Some(NextState::Election(votes));
        }

        let final_state = raft.role();

        if final_state == RaftRole::Follower {
            // neither heartbeat nor election, mean this peer is Leader before, but 
            // found it is not Leader anymore.
            (final_state, next.unwrap_or(NextState::NotQuorum))
        } else {
            // if not votes generated and also not heartbeat generate after tick, 
            // this peer is still Leader, means there must be standalone candidate election,
            // or standalone Leader try heartbeat.
            (final_state, next.unwrap_or(NextState::Finished))
        }
    }
}