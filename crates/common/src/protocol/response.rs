use crate::errors::{
    Error as ConsensusError, Result as RaftResult, StorageError,
    application::{YuError, Yusult}
};
use serde::{Deserialize, Serialize};
use std::io::ErrorKind;

pub const SUCCESS: u16 = 20000;
pub const UNKNOWN: u16 = 60000;

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct Response<T> {
    /// code ok or failure, max code is 65535
    code: u16,
    /// response hint.
    hint: String,
    content: T,
}

impl<T> Response<T>
where
    T: Default,
{
    pub fn ok(content: T) -> Self {
        Self {
            code: SUCCESS,
            hint: "ok".to_owned(),
            content,
        }
    }

    pub fn failure<H: ToString>(hint: H) -> Self {
        Self {
            code: UNKNOWN,
            hint: hint.to_string(),
            ..Default::default()
        }
    }

    pub fn failure_with_code(code: u16, hint: String) -> Self {
        let code = if Self::validate_code(code) {
            code
        } else {
            UNKNOWN
        };
        Self {
            code,
            hint,
            ..Default::default()
        }
    }

    #[inline]
    pub fn is_ok(&self) -> bool {
        self.code == SUCCESS
    }

    /// Check if the code is valid for error `Response`.
    pub fn validate_code(code: u16) -> bool {
        (code >= RAFT_NOTHING_READY && code <= RAFT_STORE_ERROR) || // in segment of raft error
        (code >= IO_NOT_FOUND && code <= IO_OTHER) || // in segment of io error
        (code >= YU_NOT_SUCH_NODE && code <= YU_ABORT) // in segment of yu error
    }
}

pub mod error_code {
    // code for raft normal error
    pub const RAFT_NOTHING_READY: u16 = 40000;
    pub const RAFT_STEP_LOCAL: u16 = 40001;
    pub const RAFT_PEER_NOT_FOUND: u16 = 40002;
    pub const RAFT_PROPOSAL_DROPPED: u16 = 40003;
    pub const RAFT_NOT_REACH_QUORUM: u16 = 40004;
    /// this is not error or not absolutely error, some time
    /// this error will be threw when raft backup
    /// can not completed as soon as possible
    pub const RAFT_REQUEST_IN_PENDING: u16 = 40005;

    // code for raft storage errors
    /// entry's index lower than first index of raftlog
    pub const RAFT_STORE_COMPACTED: u16 = 40010;
    pub const RAFT_STORE_EXPIRED_SNAP: u16 = 40011;
    pub const RAFT_STORE_SNAP_UNAVAILABLE: u16 = 40012;
    pub const RAFT_STORE_UNAVAILABLE: u16 = 40013;
    pub const RAFT_STORE_DIS_COUNTINUOUS: u16 = 40014;
    pub const RAFT_STORE_ERROR: u16 = 40015;

    // for IO error
    pub const IO_NOT_FOUND: u16 = 41001;
    pub const IO_PERM_DENIED: u16 = 41002;
    pub const IO_CONN_REFUSED: u16 = 41003;
    pub const IO_CONN_RST: u16 = 41004;
    pub const IO_CONN_ABT: u16 = 41005;
    pub const IO_NOT_CONN: u16 = 41006;
    pub const IO_ADDR_IN_USE: u16 = 41007;
    pub const IO_ADDR_UNAVAILABLE: u16 = 41008;
    pub const IO_BROKEN_PIPE: u16 = 41009;
    pub const IO_ALREADY_EXISTS: u16 = 41010;
    pub const IO_WOULD_BLOCK: u16 = 41011;
    pub const IO_INVALID_INPUT: u16 = 41012;
    pub const IO_INVALID_DATA: u16 = 41013;
    pub const IO_TIMEOUT: u16 = 41014;
    pub const IO_WRITE_ZERO: u16 = 41015;
    pub const IO_INTERRUPTED: u16 = 41016;
    pub const IO_UNEXPECTED_EOF: u16 = 41017;
    pub const IO_OTHER: u16 = 41018;
    
    // for yu error
    pub const YU_NOT_SUCH_NODE: u16 = 42001;
    pub const YU_CODEC_ERR: u16 = 42002;
    pub const YU_BALANCE_FAILED: u16 = 42003;
    pub const YU_ABORT: u16 = 42004;
}

use error_code::*;

use super::{NodeID, GroupID};

//==================================
//      Mapping errors to Response
//==================================
impl<T: Default> Into<Response<T>> for RaftResult<T> {
    fn into(self) -> Response<T> {
        match self {
            Ok(result) => Response::ok(result),
            Err(err) => err.into(),
        }
    }
}

impl<T> Into<RaftResult<T>> for Response<T>
where
    T: Default,
{
    fn into(self) -> RaftResult<T> {
        let Response { code, .. } = &self;
        if *code == SUCCESS {
            Ok(self.content)
        } else {
            let err: ConsensusError = self.into();
            Err(err)
        }
    }
}

impl<T> From<ConsensusError> for Response<T>
where
    T: Default,
{
    fn from(err: ConsensusError) -> Self {
        (&err).into()
    }
}

impl<T> From<&ConsensusError> for Response<T>
where
    T: Default,
{
    fn from(err: &ConsensusError) -> Self {
        match err {
            ConsensusError::Nothing => Self {
                code: RAFT_NOTHING_READY,
                hint: err.to_string(),
                ..Default::default()
            },
            ConsensusError::StepLocalMsg => Self {
                code: RAFT_STEP_LOCAL,
                hint: err.to_string(),
                ..Default::default()
            },
            ConsensusError::StepPeerNotFound => Self {
                code: RAFT_PEER_NOT_FOUND,
                hint: err.to_string(),
                ..Default::default()
            },
            ConsensusError::ProposalDropped(_) => Self {
                code: RAFT_PROPOSAL_DROPPED,
                hint: err.to_string(),
                ..Default::default()
            },
            ConsensusError::NotReachQuorum => Self {
                code: RAFT_NOT_REACH_QUORUM,
                hint: err.to_string(),
                ..Default::default()
            },
            ConsensusError::Pending => Self {
                code: RAFT_REQUEST_IN_PENDING,
                hint: err.to_string(),
                ..Default::default()
            },
            ConsensusError::Store(store_err) => match store_err {
                StorageError::Compacted => Self {
                    code: RAFT_STORE_COMPACTED,
                    hint: store_err.to_string(),
                    ..Default::default()
                },
                StorageError::SnapshotOutOfDate => Self {
                    code: RAFT_STORE_EXPIRED_SNAP,
                    hint: store_err.to_string(),
                    ..Default::default()
                },
                StorageError::SnapshotTemporarilyUnavailable => Self {
                    code: RAFT_STORE_SNAP_UNAVAILABLE,
                    hint: store_err.to_string(),
                    ..Default::default()
                },
                StorageError::Unavailable => Self {
                    code: RAFT_STORE_UNAVAILABLE,
                    hint: store_err.to_string(),
                    ..Default::default()
                },
                StorageError::DisContinuous => Self {
                    code: RAFT_STORE_DIS_COUNTINUOUS,
                    hint: store_err.to_string(),
                    ..Default::default()
                },
                // Store error
                other => Self {
                    code: RAFT_STORE_ERROR,
                    hint: other.to_string(),
                    ..Default::default()
                },
            },
            other => Self::failure(other),
        }
    }
}

impl<T> Into<ConsensusError> for Response<T>
where
    T: Default,
{
    fn into(self) -> ConsensusError {
        let Self {
            code,
            hint,
            content: _,
        } = self;
        return match code {
            RAFT_NOTHING_READY => ConsensusError::Nothing,
            RAFT_STEP_LOCAL => ConsensusError::StepLocalMsg,
            RAFT_PEER_NOT_FOUND | YU_NOT_SUCH_NODE => ConsensusError::StepPeerNotFound,
            RAFT_PROPOSAL_DROPPED => ConsensusError::ProposalDropped(hint),
            RAFT_NOT_REACH_QUORUM => ConsensusError::NotReachQuorum,
            RAFT_REQUEST_IN_PENDING => ConsensusError::Pending,
            RAFT_STORE_COMPACTED => ConsensusError::Store(StorageError::Compacted),
            RAFT_STORE_EXPIRED_SNAP => ConsensusError::Store(StorageError::SnapshotOutOfDate),
            RAFT_STORE_SNAP_UNAVAILABLE => {
                ConsensusError::Store(StorageError::SnapshotTemporarilyUnavailable)
            }
            RAFT_STORE_UNAVAILABLE => ConsensusError::Store(StorageError::Unavailable),
            RAFT_STORE_DIS_COUNTINUOUS => ConsensusError::Store(StorageError::DisContinuous),
            RAFT_STORE_ERROR => {
                ConsensusError::Store(StorageError::Other(Box::<
                    dyn std::error::Error + Send + Sync,
                >::from(hint)))
            }
            _ => ConsensusError::Other(Box::<dyn std::error::Error + Send + Sync>::from(hint)),
        };
    }
}

// for IO result
impl<T: Default> Into<Response<T>> for std::io::Result<T> {
    fn into(self) -> Response<T> {
        match self {
            Ok(result) => Response::ok(result),
            Err(err) => err.into(),
        }
    }
}

impl<T> Into<std::io::Result<T>> for Response<T>
where
    T: Default,
{
    fn into(self) -> std::io::Result<T> {
        let Response { code, .. } = &self;
        if *code == SUCCESS {
            Ok(self.content)
        } else {
            let err: std::io::Error = self.into();
            Err(err)
        }
    }
}

impl<T> From<std::io::Error> for Response<T>
where
    T: Default,
{
    fn from(io_err: std::io::Error) -> Self {
        match io_err.kind() {
            ErrorKind::NotFound => Self {
                code: IO_NOT_FOUND,
                hint: io_err.to_string(),
                ..Default::default()
            },
            ErrorKind::PermissionDenied => Self {
                code: IO_PERM_DENIED,
                hint: io_err.to_string(),
                ..Default::default()
            },
            ErrorKind::ConnectionRefused => Self {
                code: IO_CONN_REFUSED,
                hint: io_err.to_string(),
                ..Default::default()
            },
            ErrorKind::ConnectionReset => Self {
                code: IO_CONN_RST,
                hint: io_err.to_string(),
                ..Default::default()
            },
            ErrorKind::NotConnected => Self {
                code: IO_NOT_CONN,
                hint: io_err.to_string(),
                ..Default::default()
            },
            ErrorKind::AddrInUse => Self {
                code: IO_ADDR_IN_USE,
                hint: io_err.to_string(),
                ..Default::default()
            },
            ErrorKind::AddrNotAvailable => Self {
                code: IO_ADDR_UNAVAILABLE,
                hint: io_err.to_string(),
                ..Default::default()
            },
            ErrorKind::BrokenPipe => Self {
                code: IO_BROKEN_PIPE,
                hint: io_err.to_string(),
                ..Default::default()
            },
            ErrorKind::AlreadyExists => Self {
                code: IO_ALREADY_EXISTS,
                hint: io_err.to_string(),
                ..Default::default()
            },
            ErrorKind::WouldBlock => Self {
                code: IO_WOULD_BLOCK,
                hint: io_err.to_string(),
                ..Default::default()
            },
            ErrorKind::InvalidInput => Self {
                code: IO_INVALID_INPUT,
                hint: io_err.to_string(),
                ..Default::default()
            },
            ErrorKind::InvalidData => Self {
                code: IO_INVALID_DATA,
                hint: io_err.to_string(),
                ..Default::default()
            },
            ErrorKind::TimedOut => Self {
                code: IO_TIMEOUT,
                hint: io_err.to_string(),
                ..Default::default()
            },
            ErrorKind::WriteZero => Self {
                code: IO_WRITE_ZERO,
                hint: io_err.to_string(),
                ..Default::default()
            },
            ErrorKind::Interrupted => Self {
                code: IO_INTERRUPTED,
                hint: io_err.to_string(),
                ..Default::default()
            },
            ErrorKind::UnexpectedEof => Self {
                code: IO_UNEXPECTED_EOF,
                hint: io_err.to_string(),
                ..Default::default()
            },
            _ => Self {
                code: IO_OTHER,
                hint: io_err.to_string(),
                ..Default::default()
            },
        }
    }
}

impl<T> Into<std::io::Error> for Response<T>
where
    T: Default,
{
    fn into(self) -> std::io::Error {
        match self.code {
            IO_NOT_FOUND => std::io::Error::new(ErrorKind::NotFound, self.hint),
            IO_PERM_DENIED => std::io::Error::new(ErrorKind::PermissionDenied, self.hint),
            IO_CONN_REFUSED => std::io::Error::new(ErrorKind::ConnectionRefused, self.hint),
            IO_CONN_RST => std::io::Error::new(ErrorKind::ConnectionReset, self.hint),
            IO_NOT_CONN => std::io::Error::new(ErrorKind::NotConnected, self.hint),
            IO_ADDR_IN_USE => std::io::Error::new(ErrorKind::AddrInUse, self.hint),
            IO_ADDR_UNAVAILABLE => std::io::Error::new(ErrorKind::AddrNotAvailable, self.hint),
            IO_BROKEN_PIPE => std::io::Error::new(ErrorKind::BrokenPipe, self.hint),
            IO_ALREADY_EXISTS => std::io::Error::new(ErrorKind::AlreadyExists, self.hint),
            IO_WOULD_BLOCK => std::io::Error::new(ErrorKind::WouldBlock, self.hint),
            IO_INVALID_INPUT => std::io::Error::new(ErrorKind::InvalidInput, self.hint),
            IO_INVALID_DATA => std::io::Error::new(ErrorKind::InvalidData, self.hint),
            IO_TIMEOUT => std::io::Error::new(ErrorKind::TimedOut, self.hint),
            IO_WRITE_ZERO => std::io::Error::new(ErrorKind::WriteZero, self.hint),
            IO_INTERRUPTED => std::io::Error::new(ErrorKind::Interrupted, self.hint),
            IO_UNEXPECTED_EOF => std::io::Error::new(ErrorKind::UnexpectedEof, self.hint),
            _ => std::io::Error::new(ErrorKind::Other, self.hint),
        }
    }
}

// for application result
impl<T: Default> Into<Response<T>> for Yusult<T> {
    fn into(self) -> Response<T> {
        match self {
            Ok(result) => Response::ok(result),
            Err(err) => err.into(),
        }
    }
}

impl<T> Into<Yusult<T>> for Response<T>
where
    T: Default,
{
    fn into(self) -> Yusult<T> {
        let Response { code, .. } = &self;
        if *code == SUCCESS {
            Ok(self.content)
        } else {
            let err: YuError = self.into();
            Err(err)
        }
    }
}

impl<T> From<YuError> for Response<T>
where
    T: Default,
{
    fn from(e: YuError) -> Self {
        match e {
            YuError::IoError(io_err) => io_err.into(),
            YuError::ConsensusError(raft_err) => raft_err.into(),
            YuError::NotSuchPeer(group, node) => Self {
                code: YU_NOT_SUCH_NODE,
                hint: format!("({}, {})", group, node),
                ..Default::default()
            },
            YuError::CodecError(codec) => Self {
                code: YU_CODEC_ERR,
                hint: codec,
                ..Default::default()
            },
            YuError::BalanceError(e) => Self {
                code: YU_BALANCE_FAILED,
                hint: e.to_string(),
                ..Default::default()
            },
            YuError::Abort => Self {
                code: YU_ABORT,
                ..Default::default()
            },
            YuError::UnknownError => Self {
                code: UNKNOWN,
                ..Default::default()
            },
        }
    }
}

impl<T> Into<YuError> for Response<T>
where
    T: Default,
{
    fn into(self) -> YuError {
        let Response { code, hint, .. } = self;
        match code {
            YU_NOT_SUCH_NODE => {
                let end = hint.len() - 1;
                let peer = hint[1..end].to_string();
                let token: Vec<_> = peer.split(',').collect();
                let wild_err = YuError::NotSuchPeer(GroupID::MAX, NodeID::MAX);
                if token.len() != 2 {
                    // invalid group node
                    wild_err
                } else {
                    let group = token[0].trim().parse::<GroupID>();
                    let node = token[1].trim().parse::<NodeID>();
                    if group.is_err() || node.is_err() {
                        wild_err
                    } else {
                        YuError::NotSuchPeer(group.unwrap(), node.unwrap())
                    }
                }
            }
            YU_CODEC_ERR => YuError::CodecError(hint),
            YU_BALANCE_FAILED => YuError::BalanceError(hint.into()),
            YU_ABORT => YuError::Abort,
            _ => YuError::UnknownError,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Response;
    use crate::errors::{application::YuError, Error as ConsensusError};

    #[test]
    fn test_encode() {
        let proposal_drop = ConsensusError::ConfChange("not reached commit".into());
        let resp: Response<()> = proposal_drop.into();
        println!("{:?}", resp);

        let nsn = YuError::NotSuchPeer(2, 12);
        let resp: Response<u64> = nsn.into();
        println!("{:?}", resp);
        let err: YuError = resp.into();
        println!("error => {:?}", err);
    }
}
