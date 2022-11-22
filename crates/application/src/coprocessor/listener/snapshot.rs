pub mod file_snapshot;
pub mod bin_snapshot;

use std::{io::Result, ops::Range};
use common::protos::raft_log_proto::Snapshot;
use components::vendor::prelude::singleton;
use super::{Acl, RaftContext};

#[derive(Debug, Clone)]
pub struct Config {
    /// determine if this snapshot implementation 
    /// should receive backups in parellel.
    pub recv_backups_parallel: bool,
    pub compute_backups_checksum: bool,
    /// defaut to "127.0.0.1" means that backup receiver 
    /// will bind at which address to receive backups. makesure
    /// the sender has this address in it's hosts (e.g. linux /etc/hosts) 
    pub bind_address: String,
    /// the receiver attempt to try listening on given port before receiving
    /// backups, but the port maybe holded by other connections. so 
    /// we'll try next port in range.
    pub try_ports: Range<u16>,
    /// default to 7, we allow different thread transferring backups at same time, 
    /// but should never larger than this limited.
    pub max_allowed_inflight_transferring: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            recv_backups_parallel: true,
            compute_backups_checksum: true,
            bind_address: "127.0.0.1".to_owned(),
            try_ports: 20070..20100,
            max_allowed_inflight_transferring: 7
        }
    }
}

singleton!(CONF, Config);

/// The snapshot listener allow developers to implement
/// how to do snapshot and transfer backups from leader
/// to follower. This listener will be registered to
/// coprocessor driver, and it's methods will be called
/// when leader found some voter in state Snapshot, and
/// decide to send snapshot to it.
///
/// ### Snapshot & Backups
/// **Snapshots** are a practical way to manage versioning
/// and create light, easily accessible data. They don’t
/// need a lot of storage space or time to create copies.
///
/// **Backups** are unique copies of a directory
/// that are stored on a different, often external, location.
/// These are large-scale performances that are done
/// periodically as protection in case of server failure
/// or data loss.
///
/// ### Toolkits
/// There has some toolkits help to implement `SnapshotListener` more easier.
/// * [FileSnapshot](crate::coprocessor::listener::snapshot::file_snapshot::FileSnapshot): 
/// An implementation of file type-based backups transfer
/// * [BinSnapshot](crate::coprocessor::listener::snapshot::bin_snapshot): 
/// An implementation of binary content type-based backups transfer
/// 
/// ### Usages
/// ```rust
/// // Step1: define your backup files handler.
/// pub struct FileHandler;
/// 
/// impl BackupSender for FileHandler {
/// 
///     // Step2: let me known if you have some things to send.
///     fn prepare_backups(&self, ctx: &RaftContext, _: &SnapshotMetadata) -> PathBuf {
///         // place your backup stuffs under "./send"
///         Path::new("./").join("send").to_path_buf()
///     }
/// }
/// 
/// impl ReceiveHandler<File> for FileHandler {
///     // Step3: as a receiver, I'll only receive what I need.
///     fn exists(&self, item: &MetaInfo) -> bool {
///         let path = Path::new(item.identify.as_str());
///         let fname = path.file_name().unwrap().to_str().unwrap();
///         let to_restore = Path::new("./").join("receive").join(fname);
///         to_restore.exists()
///     }
/// 
///     // Step4: open a file instance of `item` for writting, 
///     // there maybe has multi items 
///     fn open_writer(&self, item: &MetaInfo) -> std::io::Result<File> {
///         let path = Path::new(item.identify.as_str());
///         let fname = path.file_name().unwrap().to_str().unwrap();
///         let restore = Path::new("./").join("receive");
///         let f = OpenOptions::new()
///             .create(true)
///             .write(true)
///             .open(restore.join(fname))?;
///         Ok(f)
///     }
/// }
/// 
/// // Step5: then register your handler as "file snapshot handler"
/// let listener = FileSnapshot::new(1, FileHandler, FileHandler);
/// 
/// // Step6: then this listener can be registered to coprocessor driver
/// driver.add_listener(Listener::Snapshot(Arc::new(listener)));
/// 
///```
#[crate::async_trait]
pub trait SnapshotListener: Acl {
    /// Should handle `transfering_backups` in parallel?
    /// default to true, then process of backups transfer
    /// will be handled in a standalone async handler.
    /// If process of `transfering_backups` could finished
    /// in place, suggest set this return to true.
    #[inline]
    fn conf(&self) -> &Config {
        CONF.get(|| Config::default())
    }

    /// ### Available for
    /// This method available for **Follower**
    ///
    /// ### Introduce
    /// **Step 2**: Receive snapshot from leader then handle it.
    /// For example, if backups of snapshot is small enough,
    /// then just set it as `snapshot`'s data, follower take it out
    /// and restore it at local. Even if backup is too large to send,
    /// leader can encode it's metadata to `snapshot`'s data first, then
    /// open a standalone pipeline to receive it.
    async fn prepare_recv_backups(&self, snapshot: &mut Snapshot) -> Result<()>;

    /// ### Available for
    /// This method available for **Follower**
    /// 
    /// ### Introduce
    /// **Step 4**: When a follower prepared receive backups and return some information,
    /// the leader will keep sending backup contents to the follower after 
    /// handshake.
    async fn receiving_backups(&self, snapshot: Snapshot) -> Result<()>;

    /// ### Available for
    /// This method available for **Leader**
    ///
    /// ### Introduce
    /// **Step 1**: Prepare to send snapshot to `follower` in leader side, 
    /// maybe do something like update backups etc before do really sending backups.
    async fn prepare_send_snapshot(&self, ctx: &RaftContext, snapshot: &mut Snapshot) -> Result<()>;

    /// ### Available for
    /// This method available for **Leader**
    ///
    /// ### Introduce
    /// **Step 3**: Keep sending backups of snapshot to target `follower`
    async fn sending_backups(&self, snapshot: &mut Snapshot) -> Result<()>;
}
