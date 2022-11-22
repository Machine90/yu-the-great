use crate::{
    coprocessor::listener::{Acl, RaftContext},
    debug,
    tokio::task::JoinHandle,
    torrent::runtime,
};
use std::{fs::File, io::{Result, Error, ErrorKind}, path::PathBuf, sync::Arc};

use common::protos::raft_log_proto::{Snapshot, SnapshotMetadata};
use components::{
    torrent::fast_cp::{
        config::RecvConfig,
        sync::{cp_receiver::Receiver, handler::ReceiveHandler},
        tokio::cp_file::CpFile,
        Session,
    },
    vendor::groceries::id_gen::{IDGenerator, Snowflake},
    vendor::prelude::{id_gen::Policy, DashMap},
};

use super::{Config, SnapshotListener};
use crate::tokio::sync::RwLock;

pub trait BackupSender: Send + Sync {
    /// Handler should prepare file backups by given snap_metadata,
    /// and return backups path (could be file path or directory)
    /// to `FileSnapshot`, `FileSnapshot` will load all backups
    /// under the path and attempt to send to receiver.
    fn prepare_backups(&self, ctx: &RaftContext, snap_metadata: &SnapshotMetadata) -> PathBuf;
}

/// When decide to use `FileSnapshot`, you just need to implement the `BackupSender` and set 
/// to it. The `FileSnapshot` will help you to do really send job with `fast_cp` tool.
/// 
/// This often used in some file backups transferring, for example if we save the entries in rocksdb, 
/// when other followers request for backups, `BackupSender` should help to prepare backups by 
/// writing applied entries to a "sst_file" via "sst_file_writer", so that we can send it to 
/// this follower.
///
pub struct FileSnapshot {
    sender: Arc<dyn BackupSender>,
    receiver: Arc<dyn ReceiveHandler<File>>,

    backups_to_send: DashMap<u64, CpFile>,
    recv_tasks: DashMap<u64, JoinHandle<Result<u64>>>,
    conf: Config,
    id_generator: Arc<RwLock<Snowflake>>,
}

impl FileSnapshot {

    /// Create FileSnapshot with default configure.
    pub fn new<S, R>(node_id: u32, sender: S, receiver: R) -> Self
    where
        S: BackupSender + 'static,
        R: ReceiveHandler<File> + 'static,
    {
        // session id would not save too long
        let id_generator =
            Snowflake::from_timestamp(Policy::N20(node_id, true), 1668668356910).unwrap();
        let id_generator = Arc::new(RwLock::new(id_generator));
        let conf = Config::default();
        Self::from_exists(
            id_generator,
            Arc::new(sender),
            Arc::new(receiver),
            conf,
        )
    }

    pub fn default_id_worker(
        node_id: u32,
        sender: Arc<dyn BackupSender + 'static>,
        receiver: Arc<dyn ReceiveHandler<File> + 'static>,
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
        receiver: Arc<dyn ReceiveHandler<File> + 'static>,
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
                "too many inflight transferring! please wait."
            ));
        }
        Ok(())
    }
}

impl Acl for FileSnapshot {}

#[crate::async_trait]
impl SnapshotListener for FileSnapshot {
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

        let path = self.sender.prepare_backups(ctx, snapshot.get_metadata());
        let mut backups = CpFile::new();
        backups
            .src_dir_async(path, self.conf.compute_backups_checksum)
            .await?;
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
                debug!("not backups prepare to send");
                return Ok(());
            }
            backups.apply(ack)?;
            // TODO: do not send if backups is empty in the end.
            runtime::spawn(async move {
                // `cp` contained all backup items those prepared
                let sent = backups.flush_async().await;
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

        let receive_task = runtime::spawn_blocking(move || {
            recv_progress.block()
        });

        self.recv_tasks.insert(session.id, receive_task);
        Ok(())
    }

    async fn receiving_backups(&self, snapshot: Snapshot) -> Result<()> {
        let session = snapshot.try_get_data::<Session>()?;
        let task = self.recv_tasks.remove(&session.id);
        if let Some((_, task)) = task {
            let written = task.await??;
            crate::debug!("total received {:?} bytes backups", written);
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use std::{fs::OpenOptions, io::Write, path::Path};
    use crate::tokio::{runtime::Runtime, time::Instant};
    use components::{
        torrent::fast_cp::MetaInfo,
    };

    use super::*;

    fn _snap_dir<P: AsRef<Path>>(child: P) -> PathBuf {
        let dir = std::env::current_dir().expect("unexpected dir");
        let dir = dir.join("assets").join("snapshot").join(child);
        if !dir.exists() {
            std::fs::create_dir_all(&dir).expect("unable to create snapshot directory");
        }
        dir
    }

    pub struct FileHandler;

    impl BackupSender for FileHandler {
        fn prepare_backups(&self, ctx: &RaftContext, _: &SnapshotMetadata) -> PathBuf {
            if let Some(_) = ctx.partition.as_ref() {
                // TODO: make snapshot of entries in given key-values
            }
            _snap_dir("send").to_path_buf()
        }
    }

    impl ReceiveHandler<File> for FileHandler {
        fn exists(&self, item: &MetaInfo) -> bool {
            let path = Path::new(item.identify.as_str());
            let fname = path.file_name().unwrap().to_str().unwrap();
            let restore = _snap_dir("receive").join(fname);
            restore.exists()
        }

        fn open_writer(&self, item: &MetaInfo) -> std::io::Result<File> {
            let path = Path::new(item.identify.as_str());
            let fname = path.file_name().unwrap().to_str().unwrap();

            let restore = _snap_dir("receive");
            let f = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(restore.join(fname))?;
            Ok(f)
        }

        #[inline]
        fn complete(&self, _file: &mut File) {
            // then do something such like sst_file restore etc..
        }
    }

    #[test]
    fn mock_file() {
        let path = _snap_dir("send");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path.join("backups.txt"))
            .unwrap();
        // 512 MB
        let _ = file.write(vec![0; 536870912].as_slice());
        let _ = file.flush();
    }

    #[test]
    fn test_snap() {
        let snapst = FileSnapshot::new(1, FileHandler, FileHandler);

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
