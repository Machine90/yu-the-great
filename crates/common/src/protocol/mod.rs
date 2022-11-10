pub mod response;
pub mod read_state;
pub mod proposal;

pub type NodeID = u64;
pub type GroupID = u32;
pub type PeerID = (GroupID, NodeID);
