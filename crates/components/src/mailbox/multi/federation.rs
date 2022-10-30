use std::sync::Arc;

use common::{metrics::sys_metrics::SysMetrics, protocol::NodeID, protos::multi_proto::{MultiGroup, Assignments}};

pub type FederationRef = Arc<dyn Federation + 'static>;

/// Federation is an abstract concept for multi-raft, which
/// manage all nodes of cluster and help to schedule all groups
/// on these nodes. All nodes of federation will report it's
/// information to federation.
#[async_trait::async_trait]
pub trait Federation: Send + Sync {
    /// Report this node's system info to federation.
    fn report_node_info(&self, node_id: NodeID, sys_metrics: &SysMetrics);

    /// Report groups on this node to `Federation` period.
    async fn report_groups(&self, node_id: NodeID, groups: MultiGroup) -> Assignments;
}
