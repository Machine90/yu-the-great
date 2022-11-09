//! ## Definition
//! Current Peer handle [MsgAppend](protos::raft_payload_proto::MessageType::MsgAppend)
//! and [MsgSnapshot](protos::raft_payload_proto::MessageType::MsgSnapshot)
//! &as [Follower](consensus::raft::raft_role::RaftRole::Follower)

use consensus::raft_node::{raft_process::RaftProcess, status::Status, SnapshotStatus, raft_functions::RaftFunctions};

use crate::{
    coprocessor::{driver::ApplyState, ChangeReason, listener::RaftContext},
    mailbox::api::MailBox,
    peer::{
        process::{at_most_one_msg, exactly_one_msg},
        Peer,
    },
    torrent::runtime,
    ConsensusError, RaftMsg,
    RaftMsgType::MsgSnapshot,
    RaftResult,
};

impl Peer {

    /// When calling this method as `follower`, current peer will send a msg
    /// in type `MsgAppendResponse` to it's `leader` request for snapshot. 
    /// This action only available for `follower` which has leader now, and 
    /// has not pending request snapshot (means cann't repeat request).
    /// ### Example
    /// ```
    /// // assume there has raft peer: `leader` with commit 5 
    /// // and `follower` with commit 3.
    /// 
    /// // leader side.
    /// let leader_commit: u64 = leader.status().hard_state.commit;
    /// 
    /// // at follower side.
    /// let try_request = follower.request_snapshot(leader_commit).await;
    /// assert!(try_request.is_ok());
    /// ```
    pub async fn request_snapshot(&self, expected_index: u64) -> RaftResult<()> {
        let mut raft = self.wl_raft().await;
        raft.request_snapshot(expected_index)?;
        let ready = raft.get_if_ready()?;
        let mut lrd = raft.advance(ready);
        let append_resp = exactly_one_msg(lrd.take_messages());
        drop(raft);

        self.mailbox.send_append_response(append_resp).await?;
        Ok(())
    }

    /// ## Definition
    /// Recv append message as [Follower](consensus::raft::raft_role::RaftRole::Follower),
    /// then response append_resp message
    /// to sender.
    /// ### Parameters
    /// * *append*: Message in [MsgAppend](protos::raft_payload_proto::MessageType::MsgAppend) type
    /// ### Returns
    /// * *Ok(append_resp)*: Message in [MsgAppendResponse](protos::raft_payload_proto::MessageType::MsgAppendResponse) type
    /// * *Err(e)*:
    pub async fn recv_append(&self, append: RaftMsg) -> RaftResult<RaftMsg> {
        let mut raft = self.raft_group.wl_raft().await;

        let Status { soft_state, .. } = raft.status();

        let mut ready = raft.step_and_ready(append)?;
        let commit_in_hs = self.core.persist_ready(&mut ready).await?;
        if !ready.committed_entries().is_empty() {
            self.apply_commit_entry(&mut raft, ready.take_committed_entries())
                .join_all()
                .await;
        }

        if let Some(ss) = ready.soft_state_ref() {
            self.on_soft_state_change(&soft_state, ss, ChangeReason::RecvAppend)
                .await;
        }

        let mut lr = raft.advance(ready);
        let committed = self.core.persist_light_ready(&mut lr).await?;
        drop(raft);
        let committed = committed.or(commit_in_hs);
        if let Some(commit) = committed {
            crate::trace!("follower commit index {:?}", commit);
        }
        let append_resp = at_most_one_msg(lr.take_messages());
        if append_resp.is_none() {
            return Err(ConsensusError::Nothing);
        }
        Ok(append_resp.unwrap())
    }

    /// ## Definition
    /// Recv snapshot as [Follower](consensus::raft::raft_role::RaftRole::Follower) type
    /// and try applying to current's [Storage](consensus::storage::Storage).
    ///
    /// ### Parameters
    /// * *snap*: snapshot [MsgSnapshot](protos::raft_payload_proto::MessageType::MsgSnapshot) before sycning backup.
    /// ### Returns
    /// * *Ok*: Message in [MsgAppendResponse](protos::raft_payload_proto::MessageType::MsgAppendResponse) type
    /// * *Err(e)*:
    pub async fn maybe_apply_snapshot(&self, snap: RaftMsg) -> RaftResult<RaftMsg> {
        assert_eq!(
            snap.msg_type(),
            MsgSnapshot,
            "require for msg type MsgSnapshot"
        );
        let group_id = self.get_group_id();
        let snap_from = snap.from;

        let mut raft = self.wl_raft().await;

        let ctx = RaftContext::from_status(group_id, raft.status());
        raft.step(snap)?;
        // maybe get nothing ready if snap's term is low
        let mut ready = raft.get_if_ready()?;

        let mut apply_snapshot = None;
        if let Some(snapshot) = ready.some_snapshot() {
            let apply_state = self.apply_snapshot(&ctx, snap_from, snapshot).await?;
            apply_snapshot = Some(apply_state);
        }

        // try persist if has some ready
        self.persist_ready(&mut ready).await?;

        let mut light = raft.advance(ready);
        self.persist_light_ready(&mut light).await?;
        drop(raft);

        let response = exactly_one_msg(light.take_messages());

        if let Some(apply_state) = apply_snapshot {
            // receive snapshot and handled it.
            match apply_state {
                ApplyState::Applied(status) => {
                    self.report_snap_status(status, false).await?;
                    Ok(response)
                }
                ApplyState::Applying => Err(ConsensusError::Pending),
            }
        } else {
            // receive low term snapshot from leader when support
            // pre_vote, then decide to response with latest commit.
            Ok(response)
        }
    }

    pub async fn report_snap_status(
        &self,
        status: SnapshotStatus,
        should_sync: bool,
    ) -> RaftResult<()> {
        let self_id = self.node_id();
        let leader_id = self.rl_raft().await.leader_id();
        if should_sync {
            self.mailbox
                .report_snap_status(self_id, leader_id, status.to_string())
                .await
        } else {
            let mailbox = self.mailbox.clone();
            runtime::spawn(async move {
                let resp = mailbox
                    .report_snap_status(self_id, leader_id, status.to_string())
                    .await;
                if let Err(e) = resp {
                    crate::error!(
                        "failed to report {:?} to leader: {:?}, see: {:?}",
                        status,
                        leader_id,
                        e
                    );
                }
            });
            Ok(())
        }
    }
}