use crate::{
    coprocessor::listener::{Acl, RaftContext},
    info,
    tokio::task::JoinHandle,
    torrent::runtime,
};
use std::{fs::File, io::Result, path::PathBuf, sync::Arc};

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

pub struct FileSnapshot {
    sender: Arc<dyn BackupSender>,
    receiver: Arc<dyn ReceiveHandler<File>>,

    backups_to_send: DashMap<u64, CpFile>,
    recv_tasks: DashMap<u64, JoinHandle<Result<u64>>>,
    conf: Config,
    id_generator: Arc<RwLock<Snowflake>>,
}

impl FileSnapshot {
    pub fn new<S, R>(node_id: u32, sender: S, receiver: R) -> Self
    where
        S: BackupSender + 'static,
        R: ReceiveHandler<File> + 'static,
    {
        // session id would not save too long
        let id_generator =
            Snowflake::from_timestamp(Policy::N20(node_id, true), 1659667909807).unwrap();
        let id_generator = Arc::new(RwLock::new(id_generator));
        Self::from_exists(
            id_generator,
            Arc::new(sender),
            Arc::new(receiver),
            Config::default(),
        )
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
                info!("not backups prepare to send");
                return Ok(());
            }
            backups.apply(ack)?;
            // TODO: do not send if backups is empty in the end.
            runtime::spawn(async move {
                // `cp` contained all backup items those prepared
                let sent = backups.flush_async().await;
                info!("send backups {:?}", sent);
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

        // TODO: replace with configured value
        recv_progress.set_config(RecvConfig {
            bind_host: "127.0.0.1".to_owned(),
            port_range: 20070..20100,
            ..Default::default()
        });
        recv_progress.accept_mut(&mut session)?;
        snapshot.set_data(&session)?;

        // TODO: maybe nothing attempt to be received after accept session.
        // thus do not spawn any tasks if in this case.
        if session.items.is_empty() {
            info!("attempt to receive nothing, backups is ready");
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
            crate::debug!("totally success received {:?} bytes backups", written);
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

    const TEST_PATH: &str = "C:\\workspace\\raft\\Dayu\\yu-the-great\\assets";

    pub struct FileHandler;

    impl BackupSender for FileHandler {
        fn prepare_backups(&self, ctx: &RaftContext, _: &SnapshotMetadata) -> PathBuf {
            if let Some(_) = ctx.partition.as_ref() {
                // TODO: do snapshot of key-values in range
            }
            Path::new(TEST_PATH).join("send").to_path_buf()
        }
    }

    impl ReceiveHandler<File> for FileHandler {
        fn exists(&self, item: &MetaInfo) -> bool {
            let path = Path::new(item.identify.as_str());
            let fname = path.file_name().unwrap().to_str().unwrap();
            let restore = Path::new(TEST_PATH).join("receive").join(fname);
            restore.exists()
        }

        fn open_writer(&self, item: &MetaInfo) -> std::io::Result<File> {
            let path = Path::new(item.identify.as_str());
            let fname = path.file_name().unwrap().to_str().unwrap();

            let restore = Path::new(TEST_PATH).join("receive");
            let f = OpenOptions::new()
                .create(true)
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
    fn bigfile() {
        let path = Path::new("C:\\workspace\\raft\\Dayu\\yu-the-great\\assets\\send");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path.join("backups.txt"))
            .unwrap();
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
