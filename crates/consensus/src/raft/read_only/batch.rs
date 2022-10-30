use std::collections::VecDeque;
use crate::{HashMap, HashSet};
use super::{ReadIndexStatus, ReadPolicy};
use crate::protos::raft_payload_proto::Message;

/// Batch read for `ReadOnly`. When in this mode, all applied `read_index_state`
/// will be take out when `advance` in hearbeat response, and `last_pending_read_ctx`
/// will be broadcast in heartbeat, that means raft may handle read_index async,
/// and read_index receiver should waiting for read_state ready.
/// ### Example
/// ```
/// // for exmaple in a quorum with 5 voters [1,2,3,4,5]
/// let mut read_only = BatchRead::default();
/// let my_id = 1;
/// // current node receive 3 read_index in order
/// let (rx1, mut tx1) = oneshot::channel();
/// let (rx2, mut tx2) = oneshot::channel();
/// let (rx3, mut tx3) = oneshot::channel();
/// for rx in rx1..rx3 {
///     // waiting for tx to notification in async
///     spawn(async move {
///         let readed = rx.await;
///     });
/// }
/// read_only.add_request(commit, "read key1", my_id);
/// read_only.add_request(commit, "read key2", my_id);
/// read_only.add_request(commit, "read key3", my_id);
/// // broadcast read key1 to key3 in heartbeat after a while
/// // ...
/// // then assume "read key2" reached quorum read first
/// read_only.recv_ack(voters[2,3,4,5], "read key3");
/// // "read key1" and "read key2" will be advanced.
/// for rss in read_only.advance() {
///     // tx of "read key1" and "read key2"
///     let mut tx_for_rss = todo!("find related tx here");
///     let read_ctx = &rss.req.entries[0].data[..];
///     tx_for_rss.send(todo!("do read with `read_ctx` here"));
/// }
/// ```
#[derive(Default)]
pub struct BatchRead {
    pub pending_read_index: HashMap<Vec<u8>, ReadIndexStatus>,
    pub read_index_queue: VecDeque<Vec<u8>>,
}

impl ReadPolicy for BatchRead {

    fn add_request(&mut self, index: u64, req: Message, self_id: u64) {
        let ctx = {
            let key = &req.entries[0].data;
            if self.pending_read_index.contains_key(key) {
                return;
            }
            key.to_vec()
        };
        let mut acks = HashSet::<u64>::default();
        acks.insert(self_id);
        let status = ReadIndexStatus { req, index, acks };
        self.pending_read_index.insert(ctx.clone(), status);
        self.read_index_queue.push_back(ctx);
    }

    fn recv_ack(&mut self, id: u64, ctx: &[u8]) -> Option<&crate::HashSet<u64>> {
        self.pending_read_index.get_mut(ctx).map(|rs| {
            rs.acks.insert(id);
            &rs.acks
        })
    }

    fn advance(&mut self, ctx: &[u8]) -> Vec<ReadIndexStatus> {
        
        let mut rss = vec![];
        if let Some(i) = self.read_index_queue.iter().position(|x| {
            if !self.pending_read_index.contains_key(x) {
                panic!("cannot find correspond read state from pending map");
            }
            *x == ctx
        }) {
            for _ in 0..=i {
                let rs = self.read_index_queue.pop_front().unwrap();
                let status = self.pending_read_index.remove(&rs).unwrap();
                rss.push(status);
            }
        }
        rss
    }

    fn last_pending_read_ctx(&self) -> Option<Vec<u8>> {
        self.read_index_queue.back().cloned()
    }

    #[inline]
    fn pending_read_count(&self) -> usize {
        self.read_index_queue.len()
    }
}
