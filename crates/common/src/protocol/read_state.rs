use serde::{Deserialize, Serialize};


/// ReadState provides state for read only query.
/// It's caller's responsibility to send MsgReadIndex first before getting
/// this state from ready. It's also caller's duty to differentiate if this
/// state is what it requests through request_ctx, e.g. given a unique id as
/// request_ctx.
#[derive(Default, Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct ReadState {
    /// The index of the read state.
    pub index: u64,
    /// A datagram consisting of context about the request.
    pub request_ctx: Vec<u8>,
}
