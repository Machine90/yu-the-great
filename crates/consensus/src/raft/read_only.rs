pub mod batch;
pub mod normal;

use std::ops::{Deref, DerefMut};
use crate::HashSet;
use crate::protos::raft_payload_proto::Message;

/// Determines the relative safety of and consistency of read only requests.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ReadOnlyOption {
    /// Safe guarantees the linearizability of the read only request by
    /// communicating with the quorum. It is the default and suggested option.
    Safe,
    /// LeaseBased ensures linearizability of the read only request by
    /// relying on the leader lease. It can be affected by clock drift.
    /// If the clock drift is unbounded, leader might keep the lease longer than it
    /// should (clock can move backward/pause without any bound). ReadIndex is not safe
    /// in that case.
    LeaseBased,
}

impl Default for ReadOnlyOption {
    fn default() -> ReadOnlyOption {
        ReadOnlyOption::Safe
    }
}

pub(crate) type ReadState = common::protocol::read_state::ReadState;

#[derive(Default, Debug, Clone)]
pub struct ReadIndexStatus {
    pub req: Message,
    pub index: u64,
    pub acks: HashSet<u64>,
}

pub trait ReadPolicy: Send + Sync {
    /// Adds a read only request into readonly struct. 
    /// `index` is the commit index of the raft state machine when it received
    /// the read only request. 
    /// `req` is the original read only request message from the local or remote node.
    fn add_request(&mut self, index: u64, req: Message, self_id: u64);

    /// Notifies the ReadOnly struct that the raft state machine received
    /// an acknowledgment of the heartbeat that attached with the read only request
    /// context.
    fn recv_ack(&mut self, id: u64, ctx: &[u8]) -> Option<&HashSet<u64>>;

    /// Advances the read only request queue kept by the ReadOnly struct.
    /// It dequeues the requests until it finds the read only request that has
    /// the same context as the given `ctx`.
    fn advance(&mut self, ctx: &[u8]) -> Vec<ReadIndexStatus>;

    /// Returns the context of the last pending read only request in ReadOnly struct.
    fn last_pending_read_ctx(&self) -> Option<Vec<u8>>;

    /// Inflight read request number in readonly.
    fn pending_read_count(&self) -> usize;
}

// #[derive(Default, Debug, Clone)]
pub struct ReadOnly {
    pub option: ReadOnlyOption,
    pub enable_batch: bool,
    read_policy: Box<dyn ReadPolicy>,
}

impl Deref for ReadOnly {
    type Target = dyn ReadPolicy;

    fn deref(&self) -> &Self::Target {
        self.read_policy.as_ref()
    }
}

impl DerefMut for ReadOnly {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.read_policy.as_mut()
    }
}

impl ReadOnly {
    pub fn new(option: ReadOnlyOption, enable_batch: bool) -> ReadOnly {
        let read_policy: Box<dyn ReadPolicy> = if enable_batch {
            Box::new(batch::BatchRead::default())
        } else {
            Box::new(normal::NormalRead::default())
        };
        ReadOnly {
            option,
            enable_batch,
            read_policy,
        }
    }
}

#[cfg(test)] mod tests {

    use common::protos::raft_log_proto::Entry;

    use super::*;

    fn new_read<D: AsRef<[u8]>>(data: D) -> Message {
        let mut m = Message::default();
        let ent = Entry { data: data.as_ref().to_vec(), ..Default::default() };
        m.entries.push(ent);
        m.set_msg_type(common::protos::raft_payload_proto::MessageType::MsgReadIndex);
        m
    }

    fn recv<D: AsRef<[u8]>>(data: D, ro: &mut ReadOnly, voter: Vec<u64>) {
        let mut acks = None;
        let ctx = data.as_ref();
        for i in voter {
            acks = ro.recv_ack(i, ctx);
        }
        if let Some(acks) = acks {
            if acks.len() > 3 {
                println!("==>> when recv: {:?}", String::from_utf8(ctx.to_vec()));
                // acks >= n / 2 + 1;
                for r in ro.advance(ctx) {
                    println!("==>> then advance: {:?}", String::from_utf8(r.req.entries[0].data.clone()));
                }
            }
        }
    }

    #[test] fn test() {
        let mut ro = ReadOnly::new(ReadOnlyOption::Safe, true);

        let commit = 9;
        // lock
        ro.add_request(commit, new_read(b"get 1"), 1);
        // release

        // lock
        ro.add_request(commit, new_read(b"get 2"), 1);
        // release

        ro.add_request(commit + 1, new_read(b"get 3"), 1);
        ro.add_request(commit + 2, new_read(b"get 4"), 1);
        
        recv(b"get 1", &mut ro, vec![2]);
        recv(b"get 3", &mut ro, vec![2,3,4,5]);
        recv(b"get 1", &mut ro, vec![4]);
        recv(b"get 2", &mut ro, vec![2,3,4]);
        recv(b"get 1", &mut ro, vec![5]);
    }
}