use crate::{multi::node::Node, solutions::builder::multi::Builder};
use components::{storage::group_storage::GroupStorage, vendor::prelude::local_dns_lookup};
use std::sync::Arc;
use transport::rpc::{provider::RPCPostOffice, server::NodeRpcServer};

use super::StubConfig;

impl<S: GroupStorage> Builder<S> {
    #[inline]
    pub fn use_rpc_transport(mut self) -> Self {
        let node_id = self.cp_driver.conf.id;
        let post_office = Arc::new(RPCPostOffice(node_id));
        self.post_office = Some(post_office);
        self
    }
}

impl<S: GroupStorage + Clone> Node<S> {
    pub fn start_rpc(&self, config: StubConfig) -> std::io::Result<()> {
        let StubConfig {
            address,
            print_banner,
            run_as_daemon,
        } = config;
        let node = self.id;
        let socket = local_dns_lookup(address.as_str())?;
        self.start(|service| {
            let server = NodeRpcServer {
                node_id: node,
                mailbox: service,
            };
            if run_as_daemon {
                std::thread::spawn(move || {
                    let _ = server.run(socket, print_banner);
                });
            } else {
                let _ = server.run(socket, print_banner);
            }
        })?;
        Ok(())
    }
}
