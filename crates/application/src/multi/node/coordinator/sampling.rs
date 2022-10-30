use std::{collections::HashSet};

use common::{protocol::{GroupID, NodeID}, metrics::sys_metrics::{SysMetrics, Disk}};
use components::{
    storage::group_storage::GroupStorage,
    torrent::partitions::{key::RefKey, partition::Partition}, mailbox::multi::model::check::{CheckGroup, Suggestion},
};
use sysinfo::{LoadAvg, DiskExt};

use super::NodeCoordinator;

impl<S: GroupStorage> NodeCoordinator<S> {
    #[allow(unused)]
    pub fn should_check_usage(&self, partition: &Partition<GroupID>) -> bool {
        if let Some(monitor) = self.monitor() {
            let _qps = monitor.qps_of(partition.resident);
        }
        true
    }

    pub async fn sampling_groups(&self, should_report: bool) {
        let balancer = self.balancer.as_ref();
        let partitions = self.manager.partitions().new_version();

        let mut checklist = vec![];
        for (_, partition) in unsafe { partitions.visit_committed() } {
            if !self.should_check_usage(partition) {
                continue;
            }
            let group_id = partition.resident;
            let peer = self.manager.find_peer(group_id);
            if let Err(e) = peer {
                crate::warn!("{:?} when schedule sampling", e);
                continue;
            }
            // let role = peer.unwrap();
            checklist.push(CheckGroup {
                id: group_id,
                from: partition.from_key.as_left(),
                to: partition.to_key.as_right(),
                gu: None,
                suggestion: Suggestion::Unchecked
            });
        }

        // let mut balance_requests = Vec::new();
        if !checklist.is_empty() {
            // collect group usage from `balancer`
            balancer.groups_usage(&mut checklist);
            // calculate if group need balance
            let conf = &balancer.conf();
            for group in checklist.iter_mut() {
                group.check_balance(&conf);
            }
            self.handle_checked(checklist, should_report).await;
        }
    }

    #[inline]
    pub async fn handle_checked<'a>(&self, reports: Vec<CheckGroup<'a>>, should_report: bool) {
        if !should_report {
            return;
        }
        if let Some(federation) = self.federation.as_ref() {
            federation.report_groups(self.node_id(), reports)
        }
    }

    /// Find all adjacent groups (have the same voters) that meet conditions,
    /// for example groups g1, g2, g3, g4:
    ///
    /// `[["a", "d")=>(1,2,3), ["d", "g")=>(1,2,3), ["i", "k")=>(2,3,4), ["k", "m")=>(4,5,6)]`
    ///
    /// Returns should be `[[g1, g2]]`, they're adjacent and have same voters (1,2,3)
    pub fn adjacent_groups<'a, F>(
        &self,
        mut filter: F,
        groups: impl Iterator<Item = &'a CheckGroup<'a>>,
    ) -> Vec<Vec<GroupID>>
    where
        F: FnMut(&CheckGroup) -> bool,
    {
        let mut iter = groups;
        let first = iter.next();

        let mut collect: Vec<Vec<GroupID>> = Vec::new();
        if first.is_none() {
            return collect;
        }
        let first = first.unwrap();
        let mut last = self._complete_group(first);
        let mut adjacent = Vec::new();
        adjacent.push(first.id);

        while let Some(next) = iter.next() {
            if last.is_none() {
                break;
            }
            let (_, voters, tail) = last.unwrap();
            let group = next.id;
            if next.from == tail && self.topo().has_same_nodes(&group, &voters) && filter(next) {
                adjacent.push(group);
            } else {
                let add = std::mem::take(&mut adjacent);
                if add.len() > 1 {
                    // the adjancet should have at lease 2 group.
                    collect.push(add);
                }
            }
            last = self._complete_group(next);
        }
        if adjacent.len() > 1 {
            collect.push(adjacent);
        }
        collect
    }

    #[inline]
    fn _complete_group<'a>(
        &self,
        group: &'a CheckGroup,
    ) -> Option<(GroupID, HashSet<NodeID>, RefKey<'a>)> {
        let group_id = group.id;
        let nodes = self.manager.topo.copy_group_node_ids(&group_id)?;
        Some((group_id, nodes, group.to.clone()))
    }

    /// Sampling system info, including CPU, Memory, Disks etc... 
    /// The collected metrics will be send to federation if `should_report`
    /// and federation have been assigned.
    pub fn sampling_sys(&self, should_report: bool) -> Option<SysMetrics> {
        use sysinfo::{ProcessExt, System, SystemExt};

        let mut system = System::new_all();
        system.refresh_all();

        let logical_cores = num_cpus::get();
        let physical_cores = num_cpus::get_physical();

        let pid = sysinfo::get_current_pid().ok()?;
        let process = system.process(pid)?;

        let last_cpu_usage = self.sys_metrics.read().cpu_usage;
        let cur_cpu_usage = process.cpu_usage() / logical_cores as f32;
        
        let cpu_usage = if last_cpu_usage == 0. {
            cur_cpu_usage
        } else {
            (last_cpu_usage + cur_cpu_usage) / 2.
        };

        let memory_current = process.memory();
        let start_ts = process.start_time();
        let LoadAvg { one, five, fifteen } = system.load_average();
        
        let sys_disks = system.disks();
        let mut disks = Vec::with_capacity(sys_disks.len());
        for disk in sys_disks {
            disks.push(Disk {
                mount: disk.mount_point().as_os_str().to_os_string(),
                avail_bytes: disk.available_space(),
                total_bytes: disk.total_space(),
            });
        }

        let metrics = SysMetrics {
            pid,
            start_ts,
            cpu_usage,
            cpu_load: (one, five, fifteen),
            cpu_logical_cores: logical_cores as u16,
            cpu_physical_cores: physical_cores as u16,
            memory_current,
            memory_total: system.total_memory(),
            memory_used: system.used_memory(),
            memory_avail: system.free_memory(),
            swap_total: system.total_swap(),
            swap_used: system.used_swap(),
            swap_avail: system.free_swap(),
            disks,
        };

        if should_report {
            if let Some(federation) = self.federation.as_ref() {
                federation.report_node_info(self.node_id(), &metrics);
            }
        }
        
        *self.sys_metrics.write() = metrics.clone();
        Some(metrics)
    }
}
