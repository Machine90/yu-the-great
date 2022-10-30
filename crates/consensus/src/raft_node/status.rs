use std::collections::HashSet;

use crate::protos::raft_log_proto::HardState;
use crate::protos::raft_payload_proto::{StatusProto, ProgressProto};

use crate::raft::raft_role::RaftRole;
use crate::raft::{Raft, SoftState, raft_tracker::ProgressTracker};
use crate::storage::Storage;

#[derive(Default)]
pub struct Status<'a> {
    pub id: u64,
    pub hard_state: HardState,
    pub soft_state: SoftState,
    pub applied_index: u64,
    pub tracker: Option<&'a ProgressTracker>,
}

impl<'a> Status<'a> {
    pub fn new<S: Storage>(raft: &'a Raft<S>) -> Status<'a> {
        let mut status = Status {
            id: raft.id,
            ..Default::default()
        };
        status.soft_state = raft.soft_state();
        status.hard_state = raft.hard_state();
        status.applied_index = raft.raft_log.get_applied();
        if status.soft_state.raft_state == RaftRole::Leader {
            status.tracker = Some(raft.tracker());
        }
        status
    }

    #[inline]
    pub fn leader_id(&self) -> u64 {
        self.soft_state.leader_id
    }

    #[inline]
    pub fn role(&self) -> RaftRole {
        self.soft_state.raft_state
    }

    #[inline]
    pub fn is_leader(&self) -> bool {
        self.role() == RaftRole::Leader
    }

    pub fn all_voters(&self) -> Option<HashSet<u64>> {
        if let Some (tracker) = self.tracker {
            Some(tracker.all_voters().map(|id| id).collect())
        } else {
            None
        }
    }
}

impl Into<StatusProto> for Status<'_> {
    fn into(self) -> StatusProto {
        let role = self.soft_state.raft_state.to_string();
        let leader_id = self.leader_id();
        // only available for leader
        let state_machine = self.tracker.map(|tracker| {
            let cs = tracker.to_conf_state();
            
            let mut collected = Vec::with_capacity(tracker.all_progress().len());
            for (id, progress) in tracker.all_progress().iter() {
                
                let prog = ProgressProto {
                    node_id: *id,
                    matched: progress.match_index,
                    next: progress.next_index,
                    committed_index: progress.committed_index,
                    pending_request_snapshot: progress.pending_request_snapshot(),
                    pending_snapshot: progress.pending_snapshot,
                    recent_active: progress.recent_active,
                    probe_sent: progress.probe_sent,
                    state: progress.state.to_string(),
                };
                collected.push(prog);
            }
            (Some(cs), collected)
        });
        
        let (cs, voter_progress) = state_machine.unwrap_or((None, Vec::new()));

        let hs = &self.hard_state;
        StatusProto {
            node_id: self.id,
            role,
            leader_id,
            commit: hs.commit,
            term: hs.term,
            last_vote: hs.vote,
            applied: self.applied_index,
            conf_state: cs,
            voter_progress,
            ..Default::default()
        }
    }
}
