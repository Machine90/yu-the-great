pub mod sys_metrics;

use crate::protocol::GroupID;
use key::*;

#[allow(unused)]
pub mod key {
    pub const GROUP_W_THROUGHPUT: &'static str = "G_TP_W_";
    pub const GROUP_REQ: &'static str = "G_REQ_";

    pub const TOTAL_W_THROUGHPUT: &'static str = "T_TP_W";
    pub const TOTAL_REQ: &'static str = "T_REQ";
}

#[inline]
pub fn write_throughput_of(group: GroupID) -> String {
    format!("{GROUP_W_THROUGHPUT}{group}")
}

#[inline]
pub fn requests_of(group: GroupID) -> String {
    format!("{GROUP_REQ}{group}")
}
