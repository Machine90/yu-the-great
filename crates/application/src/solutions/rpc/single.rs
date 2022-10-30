use std::sync::Arc;

use crate::{single::Node, solutions::builder::single::Builder};
use components::{storage::group_storage::GroupStorage, vendor::prelude::local_dns_lookup};
use transport::rpc::{provider::RPCPostOffice, server::NodeRpcServer};

use super::StubConfig;

impl<S: GroupStorage + Clone> Builder<S> {
    /// Default to use [tarpc](transport::rpc::tarpc_ext::tcp::rpc::tarpc)
    /// as transport framework.
    #[inline]
    pub fn use_rpc_transport(mut self) -> Self {
        let node_id = self.cp_driver_builder.conf.id;
        let post_office = Arc::new(RPCPostOffice(node_id));
        self.post_office = Some(post_office);
        self
    }
}

impl Node {
    pub fn start_rpc(&self, config: StubConfig) -> std::io::Result<()> {
        let StubConfig {
            address,
            print_banner,
            run_as_daemon,
        } = config;

        let node = self.conf().id;
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
