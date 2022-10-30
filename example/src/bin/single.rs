use application::{
    coprocessor::{
        listener::{proposal::RaftListener, Acl, RaftContext},
        ChangeReason,
    },
    peer::{
        config::NodeConfig,
        facade::{local::LocalPeer, AbstractPeer},
    },
    solutions::{builder::single::{provider, Builder}, rpc::StubConfig},
    SoftState,
};

use components::{
    common::protocol::{read_state::ReadState, NodeID},
    mailbox::{topo::PeerID, RaftEndpoint},
    monitor::MonitorConf,
    protos::{raft_group_proto::GroupProto, raft_log_proto::Entry},
    tokio1::{
        sync::mpsc::{channel, Receiver, Sender},
        time::Instant,
    },
    torrent::runtime,
    utils::endpoint_change::{ChangeSet, EndpointChange},
    vendor::prelude::{
        conf_file_logger, init_logger_factory, DashSet, LogFactory, LogLevel, LoggerConfig,
    },
};
use std::{io::Result, time::Duration};

pub fn set_logger(level: LogLevel, file_log: bool) {
    // create a default factory
    let mut factory = LogFactory::default();
    let logger = if file_log {
        conf_file_logger("./single_raft.log", LoggerConfig::in_level(level))
    } else {
        LoggerConfig::in_level(level).into()
    };
    // can change default logger that macros used
    factory.change_default_logger(logger);
    // then set it before use.
    init_logger_factory(factory);
}

struct DistributedSet {
    set: DashSet<Vec<u8>>,
    sender: Sender<NodeID>,
}

impl DistributedSet {
    pub fn new(sender: Sender<NodeID>) -> Self {
        Self {
            set: DashSet::new(),
            sender,
        }
    }
}

impl Acl for DistributedSet {}

#[application::async_trait]
impl RaftListener for DistributedSet {
    async fn handle_write(&self, _: &RaftContext, entries: &[Entry]) -> Result<i64> {
        let mut written = 0;
        for entry in entries {
            written += entry.data.len();
            self.set.insert(entry.data.to_vec());
        }
        Ok(written as i64)
    }

    async fn handle_read(&self, _: &RaftContext, read: &mut ReadState) {
        let ReadState { request_ctx, .. } = read;
        if let Some(v) = self.set.get(request_ctx) {
            read.request_ctx = v.to_vec();
        } else {
            read.request_ctx = vec![];
        }
    }

    #[allow(unused)]
    fn on_soft_state_change(
        &self,
        peer_id: PeerID,
        prev_state: SoftState,
        current_state: SoftState,
        reason: ChangeReason,
    ) {
        if current_state.leader_id != prev_state.leader_id {
            // println!("change leader to: {}", current_state.leader_id);
            let sender = self.sender.clone();
            runtime::spawn(async move {
                sender.send(current_state.leader_id).await;
            });
        }
    }
}

fn assign_group(
    group: GroupProto,
    node: NodeID,
    mut config: NodeConfig,
) -> (LocalPeer, Receiver<NodeID>) {
    config.id = node;
    config.mailbox_port = (8080 + node) as u16;

    let (tx, rx) = channel(10);
    let node = Builder::new(group, config)
        .with_raftlog_store(provider::mem_raftlog_store)
        .use_default()
        .config_monitor(MonitorConf {
            enable_group_sampling: true,
            ..Default::default()
        })
        .add_raft_listener(DistributedSet::new(tx))
        .build()
        .unwrap();
    let cli = node.get_local_client();
    let address = node.endpoint().ori_addr.clone();
    let _ = node.start_rpc(StubConfig {
        address,
        print_banner: false,
        run_as_daemon: true,
    });

    (cli, rx)
}

fn main() {
    println!("single");
    set_logger(LogLevel::Info, true);
    let mut config = NodeConfig::default();
    config.enable_read_batch = true;

    // all the nodes could be initialize from this group.
    let mut group = GroupProto {
        id: 1,
        endpoints: vec![
            RaftEndpoint::parse(1, "raft://localhost:8081")
                .unwrap()
                .into(),
            RaftEndpoint::parse(2, "raft://localhost:8082")
                .unwrap()
                .into(),
            RaftEndpoint::parse(3, "raft://localhost:8083")
                .unwrap()
                .into(),
        ],
        confstate: Some(vec![1, 2, 3].into()),
        ..Default::default()
    };

    let mut read_task = vec![];
    let mut clients = vec![];
    let mut rx = None;

    for i in 1..=3 {
        let (client, recv) = assign_group(group.clone(), i, config.clone());
        clients.push(client);
        rx = Some(recv);
    }

    group.endpoints.push(
        RaftEndpoint::parse(4, "raft://localhost:8084")
            .unwrap()
            .into(),
    );
    let (n4, _) = assign_group(group, 4, config.clone());

    let leader = rx.unwrap().blocking_recv().unwrap();
    println!("now leader is: {:?}", leader);
    let transfee = (leader % 3) + 1;

    std::thread::sleep(Duration::from_millis(100));

    for i in 0..=9u64 {
        let idx = (i % 3) as usize;
        let client = clients[idx].clone();

        let rt = runtime::spawn(async move {
            // waiting until generate a leader.
            // the notify mechanism is asynchronous, wait for a while
            let key = format!("key-{i}").as_bytes().to_vec();
            let _ = client.propose_async(key.clone()).await;

            let key = format!("key-{}", i * 10).as_bytes().to_vec();
            let _ = client.propose_async(key.clone()).await;

            let time = Instant::now();
            let mut cnt = 0;
            for j in 0..5 {
                let idx = j;
                let key = format!("key-{idx}").as_bytes().to_vec();
                let keyi = client.read_async(key).await;
                if let Err(e) = keyi {
                    println!("err {:?}", e);
                } else {
                    // let ReadState { request_ctx, .. } = keyi.unwrap();
                    // println!("[{i}] ==>> read key-{idx} value: {:?}", String::from_utf8(request_ctx));
                    cnt += 1;
                }
            }
            println!(
                "thread-{:?}: cnt({cnt}) elapsed: {:?}ms",
                i,
                time.elapsed().as_millis()
            );
        });
        read_task.push(rt);
    }

    runtime::blocking(async move {
        for t in read_task {
            let _ = t.await;
        }
        let transfee_client = &clients[(transfee - 1) as usize];
        let _ = transfee_client.transfer_leader_async(transfee).await;
        let stat = transfee_client.status(false).await;
        println!("{:#?}", stat);

        let changes = ChangeSet::new();
        let _r = transfee_client
            .propose_conf_changes_async(
                changes
                    .add(EndpointChange::add_as_voter(n4.endpoint()))
                    .add(EndpointChange::remove(2))
                    .into(),
            )
            .await;
        transfee_client.monitor().map(|m| {
            let tp = m.thoughput_of(1);
            println!("tp => {tp}\nstats{:#?}", m.statistics());
        });
    });
}
