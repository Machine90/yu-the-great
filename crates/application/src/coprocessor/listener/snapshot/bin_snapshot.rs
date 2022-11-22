use crate::{
    coprocessor::listener::{Acl, RaftContext},
    debug,
    tokio::task::JoinHandle,
    torrent::runtime,
};
use std::{
    io::{Error, ErrorKind, Result},
    sync::Arc,
};

use common::protos::raft_log_proto::{Snapshot, SnapshotMetadata};
use components::{
    torrent::fast_cp::{
        config::RecvConfig,
        sync::{cp_receiver::Receiver, handler::ReceiveHandler},
        FastCp, Session,
    },
    vendor::groceries::id_gen::{IDGenerator, Snowflake},
    vendor::prelude::{id_gen::Policy, DashMap},
};

use super::{Config, SnapshotListener};
use crate::tokio::sync::RwLock;

pub type Bytes = Vec<u8>;
const BACKUP: &'static str = "backup";

pub trait BackupSender: Send + Sync {
    /// Handler should prepare file backups by given snap_metadata,
    /// and return backups path (could be file path or directory)
    /// to `FileSnapshot`, `FileSnapshot` will load all backups
    /// under the path and attempt to send to receiver.
    fn prepare_backups(&self, ctx: &RaftContext, snap_metadata: &SnapshotMetadata) -> Bytes;
}

/// A lightly and easy use binary content snapshot listener, this implement 
/// help to write binary snapshot send and receive quickly, developer should 
/// only care about how to prepare backups data and restore it at local.
/// 
/// ### Example
/// ```rust
/// // Declare a transfer here, used to make backup and restore 
/// // backups at local.
/// struct BinTransfer;
///
/// impl ReceiveHandler<Bytes> for BinTransfer {
///     fn exists(&self, _: &MetaInfo) -> bool { false }
///
///     fn open_writer(&self, _: &MetaInfo) -> std::io::Result<Bytes> {
///         // open a `Writer` so that received backups could be write
///         // to it.
///         Ok(vec![])
///     }
///
///     #[inline]
///     fn complete(&self, data: &mut Bytes) {
///         // complete loaded backup content, so consider restore 
///         // them to business store here.
///         assert_eq!(b"hello world", &data[..]);
///     }
/// }
/// 
/// impl BackupSender for BinTransfer {
///     fn prepare_backups(&self, _: &RaftContext, _: &SnapshotMetadata) -> Bytes {
///         // load your backups here, i.e. pack your key-valus 
///         // and encode to binary code.
///         b"hello world".to_vec()
///     }
/// }
pub struct BinSnapshot {
    sender: Arc<dyn BackupSender>,
    receiver: Arc<dyn ReceiveHandler<Bytes>>,

    backups_to_send: DashMap<u64, FastCp<Bytes>>,
    recv_tasks: DashMap<u64, JoinHandle<Result<u64>>>,
    conf: Config,
    id_generator: Arc<RwLock<Snowflake>>,
}

impl BinSnapshot {
    pub fn new<S, R>(node_id: u32, sender: S, receiver: R, conf: Config) -> Self
    where
        S: BackupSender + 'static,
        R: ReceiveHandler<Bytes> + 'static,
    {
        // session id would not save too long
        let id_generator =
            Snowflake::from_timestamp(Policy::N20(node_id, true), 1668668356910).unwrap();
        let id_generator = Arc::new(RwLock::new(id_generator));
        Self::from_exists(id_generator, Arc::new(sender), Arc::new(receiver), conf)
    }

    pub fn default_id_worker(
        node_id: u32,
        sender: Arc<dyn BackupSender + 'static>,
        receiver: Arc<dyn ReceiveHandler<Bytes> + 'static>,
        conf: Config,
    ) -> Self {
        let id_generator =
            Snowflake::from_timestamp(Policy::N20(node_id, true), 1659667909807).unwrap();
        let id_generator = Arc::new(RwLock::new(id_generator));

        Self::from_exists(id_generator, sender, receiver, conf)
    }

    pub fn from_exists(
        id_generator: Arc<RwLock<Snowflake>>,
        sender: Arc<dyn BackupSender + 'static>,
        receiver: Arc<dyn ReceiveHandler<Bytes> + 'static>,
        conf: Config,
    ) -> Self {
        Self {
            sender,
            receiver,
            backups_to_send: Default::default(),
            recv_tasks: Default::default(),
            conf,
            id_generator,
        }
    }

    #[inline]
    pub async fn gen_id(&self) -> u64 {
        self.id_generator.write().await.next_id()
    }

    #[inline]
    fn _try_acquire(&self) -> Result<()> {
        if self.backups_to_send.len() >= self.conf.max_allowed_inflight_transferring as usize {
            return Err(Error::new(
                ErrorKind::WouldBlock,
                "too many inflight transferring! please wait.",
            ));
        }
        Ok(())
    }
}

impl Acl for BinSnapshot {}

#[crate::async_trait]
impl SnapshotListener for BinSnapshot {
    #[inline]
    fn conf(&self) -> &Config {
        &self.conf
    }

    async fn prepare_send_snapshot(
        &self,
        ctx: &RaftContext,
        snapshot: &mut Snapshot,
    ) -> Result<()> {
        self._try_acquire()?;

        let mut backups = FastCp::<Bytes>::new();
        // try fetching backups from sender.
        let backup = self.sender.prepare_backups(ctx, snapshot.get_metadata());
        backups.write_buffer(BACKUP, backup, false);

        let mut snap = backups.get_session();
        snap.id = self.gen_id().await;
        self.backups_to_send.insert(snap.id, backups);
        snapshot.set_data(&snap)?;
        Ok(())
    }

    async fn sending_backups(&self, snapshot: &mut Snapshot) -> Result<()> {
        let ack = snapshot.try_get_data::<Session>()?;
        let backups = self.backups_to_send.remove(&ack.id);
        if let Some((_, mut backups)) = backups {
            if ack.items.is_empty() {
                debug!("not backups to send");
                return Ok(());
            }
            backups.apply(ack)?;
            runtime::spawn(async move {
                let sent = backups.send_async().await;
                debug!("send backups {:?}", sent);
            });
        }
        Ok(())
    }

    /// Startup a receiver to keep reading incoming
    /// stream and map received items to local.
    async fn prepare_recv_backups(&self, snapshot: &mut Snapshot) -> Result<()> {
        let mut session = snapshot.try_get_data::<Session>()?;

        let receiver = self.receiver.clone();
        let mut recv_progress = Receiver::from_handler(receiver);
        recv_progress.set_config(RecvConfig {
            bind_host: self.conf.bind_address.clone(),
            port_range: self.conf.try_ports.clone(),
            ..Default::default()
        });
        recv_progress.accept_mut(&mut session)?;
        snapshot.set_data(&session)?;

        // maybe nothing attempt to be received after accept session.
        // thus do not spawn any tasks if in this case.
        if session.items.is_empty() {
            debug!("nothing to receive from backups");
            return Ok(());
        }

        let receive_task = runtime::spawn_blocking(move || recv_progress.block());
        self.recv_tasks.insert(session.id, receive_task);
        Ok(())
    }

    async fn receiving_backups(&self, snapshot: Snapshot) -> Result<()> {
        let session = snapshot.try_get_data::<Session>()?;
        let task = self.recv_tasks.remove(&session.id);
        if let Some((_, task)) = task {
            let written = task.await??;
            debug!("total received {:?} bytes backups", written);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use common::protos::raft_log_proto::{Snapshot, SnapshotMetadata};
    use components::{tokio1::runtime::Runtime, torrent::fast_cp::{sync::handler::ReceiveHandler, MetaInfo}};

    use crate::coprocessor::listener::{
        snapshot::{Config, SnapshotListener},
        RaftContext,
    };

    use super::{BackupSender, BinSnapshot, Bytes};

    struct BinTransfer;

    impl ReceiveHandler<Bytes> for BinTransfer {
        fn exists(&self, _: &MetaInfo) -> bool { false }

        fn open_writer(&self, _: &MetaInfo) -> std::io::Result<Bytes> {
            // open a `Writer` so that received backups could be write
            // to it.
            Ok(vec![])
        }

        #[inline]
        fn complete(&self, data: &mut Bytes) {
            // complete loaded backup content, so consider restore 
            // them to business store here.
            assert_eq!(b"hello world", &data[..]);
        }
    }

    impl BackupSender for BinTransfer {
        fn prepare_backups(&self, _: &RaftContext, _: &SnapshotMetadata) -> Bytes {
            // load your backups here, i.e. pack your key-valus 
            // and encode to binary code.
            b"hello world".to_vec()
        }
    }

    #[test]
    fn test_bin() {
        let snapst = BinSnapshot::new(1, BinTransfer, BinTransfer, Config::default());

        let mut snap = Snapshot::default();

        Runtime::new().unwrap().block_on(async move {
            let inst = Instant::now();
            let ctx = RaftContext::default();
            let r = snapst.prepare_send_snapshot(&ctx, &mut snap).await;
            println!("step 1: {:?}", r);
            let r = snapst.prepare_recv_backups(&mut snap).await;
            println!("step 2: {:?}", r);
            let r = snapst.sending_backups(&mut snap).await;
            println!("step 3: {:?}", r);
            let r = snapst.receiving_backups(snap).await;
            println!(
                "step 4: {:?}, elapsed: {:?}ms",
                r,
                inst.elapsed().as_millis()
            );
        });
    }
}
