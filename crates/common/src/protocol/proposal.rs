use crate::errors::{Error as ConsensusError, Result as RaftResult};
use std::io::{Error, ErrorKind};

#[derive(Debug, Clone, Copy)]
pub enum Proposal {
    Commit(u64),
    Pending,
    Timeout,
}

impl TryFrom<RaftResult<u64>> for Proposal {
    type Error = ConsensusError;

    fn try_from(value: RaftResult<u64>) -> Result<Self, Self::Error> {
        match value {
            Ok(commit) => Ok(Proposal::Commit(commit)),
            Err(e) => match e {
                ConsensusError::Io(io) if io.kind() == ErrorKind::TimedOut => Ok(Proposal::Timeout),
                ConsensusError::Pending => Ok(Proposal::Pending),
                _ => Err(e),
            },
        }
    }
}

impl Into<RaftResult<u64>> for Proposal {
    fn into(self) -> RaftResult<u64> {
        match self {
            Proposal::Commit(idx) => Ok(idx),
            Proposal::Pending => Err(ConsensusError::Pending),
            Proposal::Timeout => {
                let timeout_err = Error::new(ErrorKind::TimedOut, "timeout for proposal");
                Err(ConsensusError::Io(timeout_err))
            }
        }
    }
}
