use std::collections::HashSet;
use std::fmt::Display;

use components::mailbox::topo::PeerID;
use consensus::raft::raft_role::RaftRole;
use consensus::raft_node::{status::Status, raft_process::RaftProcess};
use crate::vendor::prelude::*;

use crate::peer::pipeline::async_pipe::Pipelines;
use crate::peer::process::{msgs, at_most_one_msg, read::ReadyRead};
use crate::peer::{Core, Peer};
use crate::coprocessor::ChangeReason;
use crate::utils::drain_filter;
use crate::{RaftMsg, RaftResult, RaftMsgType::*, ConsensusError};

pub enum NextState {
    Split(RaftRole),
    ReadIndexResp(ReadyRead),
    /// finish hearbeat with follower, has this follower response correct?
    Finish(bool),
    /// heartbeat response follower still pending request snapshot.
    Snapshot(RaftMsg),
}

impl Display for NextState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let case = match self {
            NextState::Split(role) => format!("Maybe split, current role: {:?}", role),
            NextState::ReadIndexResp(_) => format!("Handle read index response"),
            NextState::Finish(success) => format!("Finish heartbeat, success? {:?}", success),
            NextState::Snapshot(_) => format!("Responser still pending snapshot"),
        };
        write!(f, "{:?}", case)
    }
}

impl Peer {
    /// ## Definitation
    /// Broadcast heartbeat from Leader to followers, then compute actual role of this peer after 
    /// receive response from follower. ReadIndex's response maybe return after broadcast heartbeat 
    /// if this heartbeat is trigger by read_index.
    /// ### Returns
    /// * Ok(result):
    ///     * RaftRole: Finalized role of current peer.
    ///     * Vec<RaftMsg>: read_index_response, only broadcast with ctx 
    ///     (read_index ctx, generated when leader recv forwarded read_index) 
    ///     could have this return.
    pub async fn broadcast_heartbeat(
        &self, 
        heartbeat: Vec<RaftMsg>,
        _: bool // keep this arg, can be used for tracing
    ) -> RaftResult<(RaftRole, HashSet<PeerID>, Option<ReadyRead>)> {
        let total = heartbeat.len();
        let mut pipelines = Pipelines::new(self.mailbox.clone());
        let core = self.core.clone();
        // first round request (pre) vote
        pipelines.broadcast_with_async_cb(heartbeat, move |follower, ack| {
            let core = core.clone();
            async move { core._handle_ack(follower, ack).await }
        });
        let acks = pipelines.join(total).await;
        let mut split_answer = 0;

        let mut read_ack = None;
        let group_id = self.get_group_id();
        let mut no_acks = HashSet::with_capacity(acks.len());
        // only one ack of broadcast may generate read_index response.
        for (follower, (state, appends)) in acks {
            self.send_append_fut(appends, "broadcast heartbeat"); // append when broadcast heartbeat
            match state {
                NextState::Split(role) => {
                    debug!("I'm split-brain in perspective of {:?}, now I'm {:?}", follower, role);
                    split_answer += 1;
                },
                NextState::ReadIndexResp(ready_to_read) => {
                    read_ack = Some(ready_to_read);
                },
                NextState::Finish(resp_correct) => {
                    // todo record to failure list if incorrect
                    if !resp_correct {
                        no_acks.insert((group_id, follower));
                    }
                },
                NextState::Snapshot(snapshot) => {
                    // send snapshot when heartbeat to follower.
                    self.send_snapshot(snapshot).await;
                },
            };
        }
        Ok((if split_answer >= total / 2 + 1 {
            RaftRole::Follower
        } else {
            RaftRole::Leader
        }, no_acks, read_ack))
    }

    /// Receive heartbeat response from a follower then handle it 
    /// at leader side. This follower maybe in Probe state or Snapshot
    /// state, then sync with it.
    pub async fn handle_hb_ack(&self, ack: RaftMsg) {
        let from = ack.from;
        let (next, appends) = self._handle_ack(from, Ok(ack)).await;
        if let Some(appends) = appends {
            self.send_append_fut(Some(appends), "receive heartbeat response");
        }
        match next {
            NextState::Snapshot(msg) => {
                self.send_snapshot(msg).await;
            },
            _ => ()
        }
    }
}

impl Core {
    /// handle ack from heartbeat (from tick or read_index), then generate some next
    /// state after step.
    /// ### Returns
    /// * NextState: heartbeat result, what should I do next step.
    /// * appends: Only NextState is ReadIndexResp can generate some append, means some followers
    /// still in probe state.
    async fn _handle_ack(&self, from: u64, ack: RaftResult<RaftMsg>) -> (NextState, Option<Vec<RaftMsg>>) {
        if let Err(e) = ack {
            return match e {
                // maybe I'm split
                ConsensusError::Nothing => (NextState::Split(self.raft_group.role().await), None),
                _ => {
                    warn!("failed to receive heart response from follower-{:?}, see error: {:?}", from, e);
                    (NextState::Finish(false), None)
                }
            };
        }
        let ack = ack.unwrap();
        let (ack_type, reject, pending_snapshot) = (ack.msg_type(), ack.reject, ack.request_snapshot);
        let (follower, rterm, rindex) = (ack.from, ack.term, ack.index);
        assert!(matches!(ack_type, MsgHeartbeatResponse | MsgAppendResponse));
        
        let mut raft = self.wl_raft().await;
        let Status { soft_state, .. } = raft.status();
        let stepped = raft.step(ack);
        if stepped.is_err() {
            warn!("failed to step heartbeat response from follower-{:?}, see error: {:?}", follower, stepped);
            return (NextState::Finish(false), None)
        }
        if !raft.has_ready() {
            // nothing in ready, just update progress of this follower in leader.
            return (NextState::Finish(true), None)
        }

        let mut ready = raft.get_ready();

        // possibly receive append response when: 
        // 1. received high-term response from follower, means I'm split.
        // 2. that follower still pending snapshot, then response with reject, 
        // means couldn't handle heartbeat.
        if ack_type == MsgAppendResponse {
            // handle resp as split leader. then role will be changed to Follower.
            crate::warn!("Receive heartbeat rejection from remote peer-{:?} in term {:?} index-{:?}", follower, rterm, rindex);
            if let Some(ss) = ready.soft_state_ref() {
                self.on_soft_state_change(&soft_state, ss, ChangeReason::MaybeSplit).await;
            }
            return if !reject { 
                // step in case 1
                let _ = raft.advance(ready);
                (NextState::Split(raft.role()), None) 
            } else {
                // step in case 2, just update progress of that follower
                let snapshot = at_most_one_msg(ready.take_messages());
                let _ = raft.advance(ready);
                drop(raft);
                
                debug!("follower-{:?} still pending request snapshot in index: {:?}", follower, pending_snapshot);
                if let Some(snapshot) = snapshot {
                    assert_eq!(snapshot.msg_type(), MsgSnapshot);
                    (NextState::Snapshot(snapshot), None)
                } else {
                    (NextState::Finish(true), None)
                }
            };
        }

        let mut ready_to_read: ReadyRead = ReadyRead::default();
        // handle heartbeat resp as normal, maybe generate some append (appends or snapshot)
        let mut appends = msgs(ready.take_messages());
        if !ready.get_read_states().is_empty() {
            // only if leader handle read_index directly (without any forwards from follower), 
            // the read states would be generated.
            ready_to_read.read_states = ready.take_read_states();
        }
        let _ = raft.advance(ready);
        drop(raft);

        let readindex_resp = drain_filter(&mut appends, |msg| {
            msg.msg_type() == MsgReadIndexResp
        }, |ridx_resp| Some(ridx_resp));

        // when recv heartbeat ack.
        ready_to_read.responses = readindex_resp;
        (NextState::ReadIndexResp(ready_to_read), Some(appends))
    }
}

