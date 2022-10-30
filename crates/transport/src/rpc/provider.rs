use common::{
    protocol::{GroupID, NodeID, PeerID},
    protos::prelude::raft_group_ext::RAFT_MAILBOX,
};
use components::mailbox::{api::GroupMailBox, topo::Topo, PostOffice};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tarpc_ext::new_client_timeout;

use super::{client::group_client::GroupRpcClient, RaftServiceClient};

pub struct RPCPostOffice(pub NodeID);

impl PostOffice for RPCPostOffice {
    #[inline]
    fn build_group_mailbox(&self, group: GroupID, topo: &Topo) -> Arc<dyn GroupMailBox> {
        Arc::new(GroupRpcClient::from_topo(group, topo.clone()))
    }

    fn establish_connection(&self, mut peers: HashSet<PeerID>, topo: &Topo) -> HashSet<PeerID> {
        let to_add = std::mem::take(&mut peers);

        for (group, node) in to_add {
            if node == self.0 {
                // ignore this node, in other word, don't connect to myself
                continue;
            }
            let added = topo.register_connector(
                &group, 
                &node, 
                RAFT_MAILBOX, 
                false, 
                |addr| {
                    new_client_timeout!(addr, RaftServiceClient, Duration::from_millis(1000))
                }
            );
            if !added {
                peers.insert((group, node));
            }
        }
        // failure set
        peers
    }
}
