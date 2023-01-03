use components::mailbox::api::MailBox;
use consensus::raft_node::{raft_process::RaftProcess, status::Status};

use crate::{peer::{Peer, process::{at_most_one_msg}}, RaftResult, RaftMsg, ConsensusError};
use crate::coprocessor::ChangeReason;

impl Peer {

    /// Receive heartbeat message and handle it in place, then 
    /// send ack via mailbox if success.
    pub async fn recv_heartbeat(&self, heartbeat: RaftMsg) {
        if let Ok(response) = self.handle_heartbeat(heartbeat).await {
            let _ = self.mailbox.send_heartbeat_response(response).await;
        }
    }

    /// ## Definition
    /// Receive and handle message in type [MsgHeartbeat](crate::RaftMsgType::MsgHeartbeat), 
    /// this approach performs as request-response process.
    /// the response may in types:
    /// * [MsgHeartbeatResponse](crate::RaftMsgType::MsgHeartbeat): handle heartbeat as normal
    /// * [MsgAppendResponse](crate::RaftMsgType::MsgAppendResponse): when still in requesting snapshot
    pub async fn handle_heartbeat(&self, heartbeat: RaftMsg) -> RaftResult<RaftMsg> {
        let mut raft = self.wl_raft().await;
        let Status { soft_state, ..} = raft.status();
        // if nothing in ready, sometimes because current follower receive
        // a message from split leader, and current cluster disable both 
        // checkquorum and pre_vote, just return NothingReady in RPC to leader.
        let mut ready = raft.step_and_ready(heartbeat)?;

        // maybe some hardstate changed
        self.persist_ready(&mut ready).await?;

        if let Some(ss) = ready.soft_state_ref() {
            // maybe old `leader` or `candidate` received heartbeat and become follower.
            // then soft state would be changed.
            self.on_soft_state_change(&soft_state, ss, ChangeReason::RecvHeartbeat).await;
        }

        let applied_by_heartbeat = self.apply_commit_entries(
            &mut raft, 
            ready.take_committed_entries()
        ).await;
        
        let mut light_ready = raft.advance_append(ready);
        if !light_ready.committed_entries().is_empty() {
            crate::error!("ignored commit entries in heartbeat");
        }
        // maybe generate some commit, then persist it.
        let _ = self.persist_light_ready(&mut light_ready).await?;
        self._advance_apply(raft, applied_by_heartbeat).await;

        // keep this code for some heartbeat delay test
        // crate::tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        at_most_one_msg(light_ready.take_messages())
            .ok_or(ConsensusError::Other("not heartbeat response generated".into()))
    }
}
