#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PortProto {
    #[prost(string, tag="1")]
    pub label: ::prost::alloc::string::String,
    #[prost(uint32, tag="2")]
    pub port: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EndpointProto {
    #[prost(uint64, tag="1")]
    pub id: u64,
    #[prost(string, tag="2")]
    pub host: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="3")]
    pub ports: ::prost::alloc::vec::Vec<PortProto>,
    #[prost(string, repeated, tag="4")]
    pub labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// A rough calculation of group's usage and flow rate from 
/// latest sampling.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Estimate {
    #[prost(uint64, tag="1")]
    pub used: u64,
    #[prost(uint64, tag="2")]
    pub total: u64,
    #[prost(int64, tag="3")]
    pub throughput_ms: i64,
}
/// The raft group metainfo, this struct
/// can be save to file in binary or text type.
/// See text format:
/// ```properties
/// id=1
/// voter=[1, 2, 3]
/// learner=[]
/// endpoints=1-raft://localhost:50071,2-raft://localhost:50072,3-raft://localhost:50073
/// ```
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GroupProto {
    #[prost(uint32, tag="1")]
    pub id: u32,
    #[prost(bytes="vec", tag="2")]
    pub from_key: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes="vec", tag="3")]
    pub to_key: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag="4")]
    pub endpoints: ::prost::alloc::vec::Vec<EndpointProto>,
    #[prost(message, optional, tag="5")]
    pub confstate: ::core::option::Option<super::raft_log_proto::ConfState>,
    #[prost(message, optional, tag="6")]
    pub estimate: ::core::option::Option<Estimate>,
}
