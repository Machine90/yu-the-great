use crate::RaftMsg;
use common::{
    protocol::{response::Response, GroupID, NodeID},
    protos::{multi_proto::BatchMessages, raft_group_proto::GroupProto, raft_log_proto::Snapshot}, 
};
use components::{mailbox::api::NodeMailbox, tokio1::runtime::Runtime, vendor::{info, debug, warn}};
use std::{net::SocketAddr, sync::Arc};
use tarpc_ext::tcp::{
    rpc::{context::Context, tarpc},
    rpc_server::{Server, ServerConfig},
};

use super::RaftService;

#[allow(unused)]
pub(crate) const BANNER: &str = r#"
                                                                          
|\     /||\     /|                                                        
( \   / )| )   ( |                                                        
 \ (_) / | |   | |                                                        
  \   /  | |   | |                                                        
   ) (   | |   | |                                                        
   | |   | (___) |                                                        
   \_/   (_______)                                                        
                                                                          
_________          _______    _______  _______  _______  _______ _________
\__   __/|\     /|(  ____ \  (  ____ \(  ____ )(  ____ \(  ___  )\__   __/
   ) (   | )   ( || (    \/  | (    \/| (    )|| (    \/| (   ) |   ) (   
   | |   | (___) || (__      | |      | (____)|| (__    | (___) |   | |   
   | |   |  ___  ||  __)     | | ____ |     __)|  __)   |  ___  |   | |   
   | |   | (   ) || (        | | \_  )| (\ (   | (      | (   ) |   | |   
   | |   | )   ( || (____/\  | (___) || ) \ \__| (____/\| )   ( |   | |   
   )_(   |/     \|(_______/  (_______)|/   \__/(_______/|/     \|   )_(   
                                                                          
"#;

#[derive(Clone)]
pub struct NodeRpcServer {
    pub node_id: NodeID,
    pub mailbox: Arc<dyn NodeMailbox>,
}

impl NodeRpcServer {
    pub fn run(&self, socket: SocketAddr, print_banner: bool) -> std::io::Result<()> {
        let service = self.clone();
        let mut listener = Server::new(ServerConfig {
            bind_address: socket, // todo replace with configure
            max_channels_per_ip: 16,
            max_frame_buffer_size: 16 * 1024 * 1024, // 16MB per request frame.
            ..Default::default()
        });

        let task = listener.start_with(service.serve(), move |addr| {
            if print_banner {
                info!("{BANNER}\nBind Raft mailbox listener at: {addr}");
            } else {
                info!("Bind Raft mailbox listener at: {addr}");
            }
        });
        let pool = self._thread_pool()?;
        pool.block_on(async move {
            let _s = task.await;
        });
        Ok(())
    }

    fn _thread_pool(&self) -> std::io::Result<Runtime> {
        let rt = crate::tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("raft-server")
            .build()?;
        Ok(rt)
    }
}

#[tarpc::server]
impl RaftService for NodeRpcServer {
    async fn assign_group(self, _: Context, expected: NodeID, group_info: GroupProto) {
        if expected != self.node_id {
            debug!("unexpected group assignment on this node");
            return;
        }
        self.mailbox.sync_with(self.node_id, group_info).await;
    }

    async fn append(self, _: Context, group: GroupID, append: RaftMsg) -> Response<RaftMsg> {
        self.mailbox.group_append(group, append).await.into()
    }

    async fn append_async(self, _: Context, group: GroupID, append: RaftMsg) {
        let result = self.mailbox.group_append_async(group, append).await;
        if let Err(e) = result {
            warn!("handle append failed, see: {:?}", e);
        }
    }

    async fn append_response(self, _: Context, group: GroupID, append_response: RaftMsg) {
        let result = self
            .mailbox
            .group_append_response(group, append_response)
            .await;
        if let Err(e) = result {
            warn!("handle append response failed, see: {:?}", e);
        }
    }

    async fn sync_snapshot(
        self,
        _: Context,
        group: GroupID,
        snapshot: RaftMsg,
    ) -> Response<RaftMsg> {
        self.mailbox.group_snapshot(group, snapshot).await.into()
    }

    async fn accepted_snapshot(
        self,
        _: Context,
        group: GroupID,
        reply_to: NodeID,
        snapshot: Snapshot,
    ) -> Response<()> {
        self.mailbox
            .group_accepted_snapshot(group, reply_to, snapshot)
            .await
            .into()
    }

    async fn request_vote(
        self,
        _: Context,
        group: GroupID,
        vote_request: RaftMsg,
    ) -> Response<RaftMsg> {
        self.mailbox
            .group_request_vote(group, vote_request)
            .await
            .into()
    }

    async fn heartbeat(self, _: Context, group: GroupID, heartbeat: RaftMsg) -> Response<RaftMsg> {
        self.mailbox
            .group_send_heartbeat(group, heartbeat)
            .await
            .into()
    }

    async fn batch_heartbeats(self, _: Context, batched: BatchMessages) {
        let _ = self.mailbox.batch_heartbeats(batched).await;
    }

    async fn heartbeat_response(self, _: Context, group: GroupID, response: RaftMsg) {
        let _ = self
            .mailbox
            .group_send_heartbeat_response(group, response)
            .await;
    }

    async fn transfer_leader_timeout(
        self,
        _: Context,
        group: GroupID,
        timeout: RaftMsg,
    ) -> Response<()> {
        self.mailbox
            .group_transfer_leader_timeout(group, timeout)
            .await
            .into()
    }

    async fn redirect_proposal(
        self,
        _: Context,
        group: GroupID,
        propose: RaftMsg,
    ) -> Response<u64> {
        self.mailbox
            .group_redirect_proposal(group, propose)
            .await
            .into()
    }

    async fn redirect_transfer_leader(
        self,
        _: Context,
        group: GroupID,
        transfer: RaftMsg,
    ) -> Response<()> {
        self.mailbox
            .group_redirect_transfer_leader(group, transfer)
            .await
            .into()
    }

    async fn redirect_read_index(
        self,
        _: Context,
        group: GroupID,
        readindex: RaftMsg,
    ) -> Response<RaftMsg> {
        self.mailbox
            .group_redirect_read_index(group, readindex)
            .await
            .into()
    }

    async fn report_snapshot(
        self,
        _: Context,
        group: GroupID,
        from: NodeID,
        leader_id: NodeID,
        status: String,
    ) -> Response<()> {
        self.mailbox
            .group_report_snap_status(group, from, leader_id, status)
            .await
            .into()
    }
}
