pub mod strictly;
pub mod simple;
pub mod batch;

use components::torrent::fast_cp::utils::write_u64;
use super::read_index_ctx::{Interested, ReadContext};

#[inline]
pub fn encode_unique_read(id: u64, read_ctx: &mut ReadContext) {
    match read_ctx.get_read_mut() {
        Interested::Directly(read_index) => {
            let mut ctx = write_u64(id).to_vec();
            ctx.append(read_index);
            *read_index = ctx;
        },
        _ => ()
    };
}

#[inline]
pub fn decode_unique_read<'a>(request_ctx: &'a [u8]) -> &'a [u8] {
    &request_ctx[8..]
}
