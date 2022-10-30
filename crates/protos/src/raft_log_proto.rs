/// The entry is a type of change that needs to be applied. It contains two data fields.
/// While the fields are built into the model; their usage is determined by the entry_type.
///
/// For normal entries, the data field should contain the data change that should be applied.
/// The context field can be used for any contextual data that might be relevant to the
/// application of the data.
///
/// For configuration changes, the data will contain the ConfChange message and the
/// context will provide anything needed to assist the configuration change. The context
/// if for the user to set and use in this case.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Entry {
    #[prost(enumeration="EntryType", tag="1")]
    pub entry_type: i32,
    #[prost(uint64, tag="2")]
    pub term: u64,
    #[prost(uint64, tag="3")]
    pub index: u64,
    #[prost(bytes="vec", tag="4")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes="vec", tag="6")]
    pub context: ::prost::alloc::vec::Vec<u8>,
    /// Deprecated! It is kept for backward compatibility.
    /// TODO: remove it in the next major release.
    #[prost(bool, tag="5")]
    pub sync_log: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConfState {
    #[prost(uint64, repeated, tag="1")]
    pub voters: ::prost::alloc::vec::Vec<u64>,
    #[prost(uint64, repeated, tag="2")]
    pub learners: ::prost::alloc::vec::Vec<u64>,
    /// The voters in the outgoing config. If not empty the node is in joint consensus.
    #[prost(uint64, repeated, tag="3")]
    pub voters_outgoing: ::prost::alloc::vec::Vec<u64>,
    /// The nodes that will become learners when the outgoing config is removed.
    /// These nodes are necessarily currently in nodes_joint (or they would have
    /// been added to the incoming config right away).
    #[prost(uint64, repeated, tag="4")]
    pub learners_next: ::prost::alloc::vec::Vec<u64>,
    /// If set, the config is joint and Raft will automatically transition into
    /// the final config (i.e. remove the outgoing config) when this is safe.
    #[prost(bool, tag="5")]
    pub auto_leave: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HardState {
    #[prost(uint64, tag="1")]
    pub term: u64,
    #[prost(uint64, tag="2")]
    pub vote: u64,
    #[prost(uint64, tag="3")]
    pub commit: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshotMetadata {
    /// The current `ConfState`.
    #[prost(message, optional, tag="1")]
    pub conf_state: ::core::option::Option<ConfState>,
    /// The applied index.
    #[prost(uint64, tag="2")]
    pub index: u64,
    /// The term of the applied index.
    #[prost(uint64, tag="3")]
    pub term: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Snapshot {
    #[prost(bytes="vec", tag="1")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag="2")]
    pub metadata: ::core::option::Option<SnapshotMetadata>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum EntryType {
    EntryNormal = 0,
    EntryConfChange = 1,
    EntryCmd = 2,
}
