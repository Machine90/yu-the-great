#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Message {
    #[prost(enumeration="MessageType", tag="1")]
    pub msg_type: i32,
    #[prost(uint64, tag="2")]
    pub to: u64,
    #[prost(uint64, tag="3")]
    pub from: u64,
    #[prost(uint64, tag="4")]
    pub term: u64,
    /// logTerm is generally used for appending Raft logs to followers. For example,
    /// (type=MsgAppend,index=100,log_term=5) means leader appends entries starting at
    /// index=101, and the term of entry at index 100 is 5.
    /// (type=MsgAppendResponse,reject=true,index=100,log_term=5) means follower rejects some
    /// entries from its leader as it already has an entry with term 5 at index 100.
    #[prost(uint64, tag="5")]
    pub log_term: u64,
    #[prost(uint64, tag="6")]
    pub index: u64,
    #[prost(message, repeated, tag="7")]
    pub entries: ::prost::alloc::vec::Vec<super::raft_log_proto::Entry>,
    #[prost(uint64, tag="8")]
    pub commit: u64,
    #[prost(uint64, tag="15")]
    pub commit_term: u64,
    #[prost(message, optional, tag="9")]
    pub snapshot: ::core::option::Option<super::raft_log_proto::Snapshot>,
    #[prost(uint64, tag="13")]
    pub request_snapshot: u64,
    #[prost(bool, tag="10")]
    pub reject: bool,
    #[prost(uint64, tag="11")]
    pub reject_hint: u64,
    #[prost(bytes="vec", tag="12")]
    pub context: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag="14")]
    pub priority: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProgressProto {
    #[prost(uint64, tag="1")]
    pub node_id: u64,
    #[prost(uint64, tag="2")]
    pub matched: u64,
    #[prost(uint64, tag="3")]
    pub next: u64,
    #[prost(uint64, tag="4")]
    pub committed_index: u64,
    #[prost(uint64, tag="5")]
    pub pending_request_snapshot: u64,
    #[prost(uint64, tag="6")]
    pub pending_snapshot: u64,
    #[prost(bool, tag="7")]
    pub recent_active: bool,
    #[prost(bool, tag="8")]
    pub probe_sent: bool,
    #[prost(string, tag="9")]
    pub state: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatusProto {
    #[prost(uint64, tag="1")]
    pub node_id: u64,
    #[prost(uint32, tag="2")]
    pub group_id: u32,
    #[prost(string, tag="3")]
    pub role: ::prost::alloc::string::String,
    #[prost(uint64, tag="4")]
    pub leader_id: u64,
    #[prost(uint64, tag="5")]
    pub commit: u64,
    #[prost(uint64, tag="6")]
    pub term: u64,
    #[prost(uint64, tag="7")]
    pub last_vote: u64,
    #[prost(uint64, tag="8")]
    pub applied: u64,
    #[prost(message, optional, tag="9")]
    pub conf_state: ::core::option::Option<super::raft_log_proto::ConfState>,
    #[prost(message, repeated, tag="10")]
    pub voter_progress: ::prost::alloc::vec::Vec<ProgressProto>,
    #[prost(message, repeated, tag="11")]
    pub endpoints: ::prost::alloc::vec::Vec<super::raft_group_proto::EndpointProto>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum MessageType {
    MsgHup = 0,
    MsgBeat = 1,
    /// leader -- local --> leader; 
    /// follower -- local --> follower -- forward --> leader
    MsgPropose = 2,
    /// leader --> follower
    MsgAppend = 3,
    /// follower --> leader
    MsgAppendResponse = 4,
    MsgRequestVote = 5,
    MsgRequestVoteResponse = 6,
    MsgSnapshot = 7,
    /// leader --> follower
    MsgHeartbeat = 8,
    /// follower --> leader
    MsgHeartbeatResponse = 9,
    /// rpc --> leader (as server), then step local.
    MsgUnreachable = 10,
    MsgSnapStatus = 11,
    /// leader -- local --> leader
    MsgCheckQuorum = 12,
    /// follower -- forward --> leader
    MsgTransferLeader = 13,
    /// leader --> follower
    MsgTimeoutNow = 14,
    /// follower -- forward --> leader
    MsgReadIndex = 15,
    /// leader -- response --> follower
    MsgReadIndexResp = 16,
    MsgRequestPreVote = 17,
    MsgRequestPreVoteResponse = 18,
}
