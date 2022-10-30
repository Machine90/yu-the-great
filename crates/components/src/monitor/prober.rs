use std::ops::Deref;

use common::{protocol::GroupID, metrics};
use torrent::dams::Window;

pub struct Prober<'a> {
    pub(super) window: &'a Window
}

impl Deref for Prober<'_> {
    type Target = Window;

    fn deref(&self) -> &Self::Target {
        &self.window
    }
}

impl Prober<'_> {
    
    #[inline(always)]
    pub fn write_group(&self, group: GroupID, bytes: i64) {
        self.window.incr(metrics::write_throughput_of(group), bytes);
    }

    #[inline(always)]
    pub fn write_node(&self, bytes: i64) {
        self.window.incr(metrics::key::TOTAL_W_THROUGHPUT, bytes);
    }

    #[inline(always)]
    pub fn query_group(&self, group: GroupID) {
        self.window.incr(metrics::requests_of(group), 1);
    }

    #[inline(always)]
    pub fn query_node(&self) {
        self.window.incr(metrics::key::TOTAL_REQ, 1);
    }
}
