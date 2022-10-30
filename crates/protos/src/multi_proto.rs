#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GroupMessage {
    #[prost(uint32, tag="1")]
    pub group: u32,
    #[prost(message, optional, tag="2")]
    pub message: ::core::option::Option<super::raft_payload_proto::Message>,
}
/// Batched messages that prepare send to 
/// the same node.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchMessages {
    #[prost(uint64, tag="1")]
    pub to: u64,
    #[prost(message, repeated, tag="2")]
    pub messages: ::prost::alloc::vec::Vec<GroupMessage>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BalanceCmd {
    #[prost(enumeration="Assignment", tag="1")]
    pub assignment: i32,
    #[prost(message, repeated, tag="2")]
    pub groups: ::prost::alloc::vec::Vec<super::raft_group_proto::GroupProto>,
    #[prost(message, repeated, tag="3")]
    pub padded_endpoints: ::prost::alloc::vec::Vec<super::raft_group_proto::EndpointProto>,
    #[prost(uint32, tag="4")]
    pub consumer: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchBalance {
    #[prost(uint64, tag="1")]
    pub id: u64,
    #[prost(message, repeated, tag="2")]
    pub cmds: ::prost::alloc::vec::Vec<BalanceCmd>,
    #[prost(bytes="vec", tag="3")]
    pub context: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Assignment {
    Split = 0,
    Create = 1,
    Compact = 2,
    Transfer = 3,
}
