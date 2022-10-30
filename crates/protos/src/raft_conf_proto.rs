#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConfChange {
    #[prost(uint64, tag="1")]
    pub node_id: u64,
    #[prost(enumeration="ConfChangeType", tag="2")]
    pub change_type: i32,
    #[prost(bytes="vec", tag="3")]
    pub context: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchConfChange {
    #[prost(uint64, tag="1")]
    pub id: u64,
    #[prost(enumeration="ConfChangeTransition", tag="2")]
    pub transition: i32,
    #[prost(message, repeated, tag="3")]
    pub changes: ::prost::alloc::vec::Vec<ConfChange>,
    #[prost(bytes="vec", tag="4")]
    pub context: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ConfChangeType {
    /// A voter has all privillages. it can take part in
    /// an election, append and read_index, leader will try 
    /// to keep consistence with a voter
    AddNode = 0,
    RemoveNode = 1,
    /// Learner can recv Append, Snapshot and Heartbeat, but can't 
    /// join the election, and can't join the commit calculation.
    AddLearnerNode = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ConfChangeTransition {
    Auto = 0,
    Implicit = 1,
    Explicit = 2,
}
