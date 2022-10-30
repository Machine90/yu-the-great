use quick_error::quick_error;

quick_error! {

    #[derive(Debug)]
    pub enum Error {
        /// An IO error occurred
        Io(err: std::io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        /// A storage error occurred.
        Store(err: StorageError) {
            from()
            cause(err)
            description(err.description())
        }
        Other(err: Box<dyn std::error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
        /// The proposal of changes was dropped.
        ProposalDropped(reason: String) {
            description("proposal dropped")
            display("{}", reason)
        }
        /// Conf change error
        ConfChange(message: String) {
            display("{}", message)
        }
        /// When stepping with a local message.
        StepLocalMsg {
            description("shouldn't step in a raft local message")
        }
        StepPeerNotFound {
            description("the peer attempt to step was not found")
        }
        RequestSnapshotDropped {
            description("the snapshot was dropped")
        }
        Nothing {
            description("there has nothing in ready and nothing to response")
        }
        NotReachQuorum {
            description("request has not reached the quorum")
            display("less than majority (n / 2 + 1) peers of consensus group response for this operation")
        }
        Pending {
            description("request still in pending")
        }
    }
}

quick_error! {
    /// An error with the storage.
    #[derive(Debug)]
    pub enum StorageError {
        /// The storage was compacted and not accessible
        Compacted {
            description("log compacted")
        }
        DisContinuous {
            description("log should be continuous")
        }
        /// The log is not available.
        Unavailable {
            description("log unavailable")
        }
        /// The snapshot is out of date.
        SnapshotOutOfDate {
            description("snapshot out of date")
        }
        /// The snapshot is being created.
        SnapshotTemporarilyUnavailable {
            description("snapshot is temporarily unavailable")
        }
        /// Some other error occurred.
        Other(err: Box<dyn std::error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
    }
}

impl PartialEq for Error {
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::match_same_arms))]
    fn eq(&self, other: &Error) -> bool {
        match (self, other) {
            (Error::Io(ref e1), Error::Io(ref e2)) => e1.kind() == e2.kind(),
            (Error::ProposalDropped(_), Error::ProposalDropped(_)) => true,
            (Error::ConfChange(e1), Error::ConfChange(e2)) => e1 == e2,
            (Error::RequestSnapshotDropped, Error::RequestSnapshotDropped) => true,
            (Error::Nothing, Error::Nothing) => true,
            (Error::Pending, Error::Pending) => true,
            (Error::StepLocalMsg, Error::StepLocalMsg) => true,
            (Error::StepPeerNotFound, Error::StepPeerNotFound) => true,
            _ => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub mod application {
    use super::*;
    use std::io::{Error as IOError, ErrorKind};
    pub type Yusult<T> = std::result::Result<T, YuError>;

    quick_error! {
        #[derive(Debug)]
        pub enum YuError {
            IoError(ie: std::io::Error) {
                from()
                description(ie.description())
                display("{:?}", &ie)
            }
            ConsensusError(ce: Error) {
                from()
                description(ce.description())
                display("{:?}", &ce)
            }
            NotSuchPeer(group_id: u32, node_id: u64) {
                description(format!("Not such Raft Node"))
                display("node-{:?} not found in group-{:?}", node_id, group_id)
            }
            CodecError(error_msg: String) {
                from()
                description(error_msg)
                display("{:?}", error_msg)
            }
            BalanceError {
                from()
                description("failed to do balance job")
            }
            Abort {
                description("abort server")
            }
            UnknownError {
                description("unknown error")
            }
        }
    }

    impl Into<std::io::Error> for YuError {
        fn into(self) -> std::io::Error {
            match self {
                YuError::IoError(io) => io,
                YuError::ConsensusError(re) => {
                    IOError::new(ErrorKind::Other, format!("raft error: {:?}", re))
                }
                YuError::NotSuchPeer(g, n) => IOError::new(
                    ErrorKind::NotFound,
                    format!("peer on node-{:?} of group-{:?} not found", n, g),
                ),
                YuError::CodecError(ce) => {
                    IOError::new(ErrorKind::InvalidInput, format!("codec error: {:?}", ce))
                }
                YuError::BalanceError => {
                    IOError::new(ErrorKind::Unsupported, "failed to do balance job")
                },
                YuError::Abort => {
                    IOError::new(ErrorKind::Other, "abort server")
                }
                YuError::UnknownError => IOError::new(ErrorKind::Other, "unknown error"),
            }
        }
    }

    impl YuError {
        pub fn not_exists<R: ToString>(reason: R) -> Self {
            Self::IoError(IOError::new(
                ErrorKind::NotFound, 
                reason.to_string()
            ))
        }
    }
}
