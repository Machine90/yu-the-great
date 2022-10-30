use super::{ReadIndexStatus, ReadPolicy};
use crate::protos::raft_payload_proto::Message;
use crate::{HashMap, HashSet};

#[derive(Default)]
pub struct NormalRead {
    pub pending_read_index: HashMap<Vec<u8>, ReadIndexStatus>,
}

impl ReadPolicy for NormalRead {
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
        self.pending_read_index.insert(ctx, status);
    }

    fn recv_ack(&mut self, id: u64, ctx: &[u8]) -> Option<&HashSet<u64>> {
        self.pending_read_index.get_mut(ctx).map(|rs| {
            rs.acks.insert(id);
            &rs.acks
        })
    }

    fn advance(&mut self, ctx: &[u8]) -> Vec<ReadIndexStatus> {
        self.pending_read_index
            .remove(ctx)
            .map(|status| vec![status])
            .unwrap_or_default()
    }

    fn last_pending_read_ctx(&self) -> Option<Vec<u8>> {
        None
    }

    #[inline]
    fn pending_read_count(&self) -> usize {
        self.pending_read_index.len()
    }
}
