#[allow(unused)]
pub mod memory;

use byteorder::{BigEndian, ByteOrder};

#[inline]
pub fn group_idx(group: u32, index: u64) -> [u8; 12] {
    let mut _group_idx = [0; 12];
    BigEndian::write_u32(&mut _group_idx[..4], group);
    BigEndian::write_u64(&mut _group_idx[4..], index);
    _group_idx
}

#[inline]
pub fn group_prefix(group: u32) -> [u8; 4] {
    let mut _group = [0; 4];
    BigEndian::write_u32(&mut _group[..4], group);
    _group
}

#[inline]
pub fn parse_group_idx(group_idx: [u8; 12]) -> (u32, u64) {
    (
        BigEndian::read_u32(&group_idx[..4]), // group_id
        BigEndian::read_u64(&group_idx[4..]), // index
    )
}

#[inline]
pub fn parse_group(group_prefix: [u8; 4]) -> u32 {
    BigEndian::read_u32(&group_prefix[..])
}
