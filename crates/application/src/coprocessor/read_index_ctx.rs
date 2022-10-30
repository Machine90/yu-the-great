use std::{fmt::Debug};

use components::{utils::pending::receiver::PendingRecv};
use common::protocol::response::Response;
use crate::{
    peer::process::read::{ReadyRead, ReadedState}, 
    RaftMsg,
    RaftMsgType::*,
    ConsensusError,
};

#[derive(Debug, Clone)]
pub(crate) enum Interested {
    Forward(Vec<u8>),
    Directly(Vec<u8>),
    HB
}

pub enum EvaluateRead {
    MaybeExist,
    HasForward(Response<RaftMsg>),
    ShouldForward(PendingRecv<Response<RaftMsg>>),
    HasReaded(Response<ReadedState>),
    ShouldRead(PendingRecv<Response<ReadedState>>),
    NotExist
}

impl Debug for EvaluateRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaybeExist => write!(f, "MaybeExist"),
            Self::HasForward(_) => write!(f, "HasForward"),
            Self::ShouldForward(_) => write!(f, "ShouldForward"),
            Self::HasReaded(_) => write!(f, "HasReaded"),
            Self::ShouldRead(_) => write!(f, "ShouldRead"),
            Self::NotExist => write!(f, "NotExist"),
        }
    }
}

impl Default for EvaluateRead {
    fn default() -> Self {
        Self::MaybeExist
    }
}

#[derive(Debug)]
pub enum MaybeReady {
    Succeed(ReadyRead),
    Error(ConsensusError),
    NotReady
}

impl MaybeReady {
    pub fn is_ready(&self) -> bool {
        match &self {
            MaybeReady::NotReady => false,
            _ => true
        }
    }
}

impl Default for MaybeReady {
    fn default() -> Self {
        MaybeReady::NotReady
    }
}

#[derive(Debug)]
pub struct ReadContext {
    pub(crate) interested: Interested,
    evaluate: EvaluateRead,
    pub(super) ready: MaybeReady,
}

impl ReadContext {

    pub fn from_read(read_index: Vec<u8>) -> Self {
        Self::_new(Interested::Directly(read_index))
    }

    pub fn from_forward(read_index: &RaftMsg) -> Self {
        assert_eq!(read_index.msg_type(), MsgReadIndex);
        assert!(read_index.entries.len() > 0);
        Self::_new(Interested::Forward(read_index.entries[0].data.to_vec()))
    }

    pub fn from_hb() -> Self {
        Self::_new(Interested::HB)
    }

    fn _new(from: Interested) -> Self {
        Self { 
            interested: from, 
            evaluate: Default::default(),
            ready: MaybeReady::NotReady, 
        }
    }

    #[inline]
    pub fn interested_read(&self) -> Option<&Vec<u8>> {
        match &self.interested {
            Interested::Forward(read) => {
                Some(read)
            },
            Interested::Directly(read) => {
                Some(read)
            },
            Interested::HB => None,
        }
    }

    #[inline]
    pub(super) fn get_read(&self) -> &Interested {
        &self.interested
    }

    #[inline]
    pub(super) fn get_read_mut(&mut self) -> &mut Interested {
        &mut self.interested
    }

    #[inline]
    pub fn with_ready(&mut self, ready: ReadyRead) -> &mut Self {
        self.ready = MaybeReady::Succeed(ready);
        self
    }

    #[inline]
    pub fn maybe_ready(&mut self, ready: MaybeReady) -> &mut Self {
        self.ready = ready;
        self
    }

    /// Take out current ready reads.
    #[inline]
    pub fn take_ready(&mut self) -> MaybeReady {
        std::mem::take(&mut self.ready)
    }

    /// Assert this read's result at current state.
    #[inline]
    pub fn assert(&mut self, evaluate: EvaluateRead) {
        self.evaluate = evaluate;
    }

    /// Evaluate current read state, maybe has some response or 
    /// readed content, then take it out.
    pub fn evaluate(&mut self) -> EvaluateRead {
        let prev = match &mut self.evaluate {
            EvaluateRead::MaybeExist => EvaluateRead::MaybeExist,
            EvaluateRead::NotExist => EvaluateRead::NotExist,
            EvaluateRead::HasForward(resp) => {
                EvaluateRead::HasForward(std::mem::take(resp))
            },
            EvaluateRead::HasReaded(readed) => {
                EvaluateRead::HasReaded(std::mem::take(readed))
            },
            EvaluateRead::ShouldForward(pending) => {
                EvaluateRead::ShouldForward(std::mem::take(pending))
            },
            EvaluateRead::ShouldRead(pending) => {
                EvaluateRead::ShouldRead(std::mem::take(pending))
            },
        };
        self.evaluate = EvaluateRead::MaybeExist;
        prev
    }

    #[inline]
    pub fn take_response(self) -> Option<Response<RaftMsg>> {
        match self.evaluate {
            EvaluateRead::HasForward(resp) => Some(resp),
            _ => None
        }
    }

    #[inline]
    pub fn take_readed(self) -> Option<Response<ReadedState>> {
        match self.evaluate {
            EvaluateRead::HasReaded(readed) => Some(readed),
            _ => None
        }
    }
}
