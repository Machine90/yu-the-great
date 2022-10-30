use std::sync::Arc;

use common::{metrics::sys_metrics::SysMetrics, protocol::NodeID};

use super::model::check::CheckGroup;

pub type FederationRef = Arc<dyn Federation + 'static>;

/// Federation is an abstract concept for multi-raft, which
/// manage all nodes of cluster and help to schedule all groups
/// on these nodes. All nodes of federation will report it's
/// information to federation.
pub trait Federation: Send + Sync {
    /// Report this node's system info to federation.
    fn report_node_info(&self, node_id: NodeID, sys_metrics: &SysMetrics);

    fn report_groups<'a>(&self, node_id: NodeID, reports: Vec<CheckGroup<'a>>);
}
