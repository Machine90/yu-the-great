use consensus::raft_node::raft_process::RaftProcess;

use crate::{
    peer::{process::read::ReadyRead, Core}, 
    ConsensusError::{ProposalDropped},
    RaftMsg, RaftMsgType::MsgReadIndexResp, RaftResult, coprocessor::{read_index_ctx::ReadContext},
};

use super::ReadedState;

impl Core {
    
    /// When folower recv forwarded read response from leader, then handle it and 
    /// collect result from `ReadState`.
    pub async fn handle_read_index_resp(
        &self, 
        mut read_ctx: ReadContext, 
        resp: RaftMsg
    ) -> RaftResult<ReadedState> {
        assert_eq!(resp.msg_type(), MsgReadIndexResp, "Only accept MsgReadIndexResp");

        let mut raft = self.raft_group.wl_raft().await;
        // when response's term lower than current peer's term, follower will ignore this forwarded read index response.
        // leader maybe changed before receive response from old leader (MsgReadIndexResp will attached old leader's term).
        // When in this case, nothing will be generated in ready. Then read_index judge to be failure with error Nothing.
        let mut ready = raft.step_and_ready(resp)?;

        // maybe exists some entries to be apply after follower forward request.
        // this mechanism guaratee the follower has applied 
        let applied_by_follower_read = self.apply_commit_entries(
            &mut raft, 
            ready.take_committed_entries()
        ).await;

        self.persist_ready(&mut ready).await?;
        // then follower handle read.
        read_ctx.with_ready(ReadyRead::rss(ready.take_read_states()));
        
        let mut light_ready = raft.advance_append(ready);
        let _commit = self.persist_light_ready(&mut light_ready).await?;

        let ctx = self._advance_apply(raft, applied_by_follower_read).await;

        self.coprocessor_driver.advance_read(&ctx, &mut read_ctx).await?;
        read_ctx.take_readed().ok_or(
            ProposalDropped("Could not found read state from read_index_response".to_owned())
        )?.into()
    }
}
