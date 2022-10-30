use crate::{raft::{DUMMY_ID, raft_tracker::{ProgressMap, ProgressTracker}}};
use crate::errors::{Error, Result};
use crate::Cluster;
use crate::protos::{raft_conf_proto::{ConfChangeType::*, ConfChange}, raft_log_proto::ConfState};

#[derive(Debug)]
pub enum ChangeType {
    AddPeer, RemovePeer
}

/// The ChangeRecord is (peer_id, change), which mark the change of the cluster<br/>
/// E.g. (peer 1, AddPeer), (peer 1, RemovePeer)
#[derive(Debug)]
pub struct ChangeRecord(u64, ChangeType);

impl ChangeRecord {

    #[inline]
    pub fn get_peer(&self) -> u64 {
        self.0
    }

    #[inline]
    pub fn get_type(&self) -> &ChangeType {
        &self.1
    }
}

/// Used to collect changes of cluster.
#[derive(Debug)]
pub struct ChangeRecords<'a> {
    base: &'a ProgressMap,
    logs: Vec<ChangeRecord>
}

impl ChangeRecords<'_> {

    #[allow(dead_code)]
    fn new(tracker: &ProgressTracker) -> ChangeRecords {
        ChangeRecords {
            base: tracker.all_progress(),
            logs: vec![]
        }
    }
    
    fn append_change(&mut self, peer_id: u64, change: ChangeType) {
        self.logs.push(ChangeRecord(peer_id, change));
    }

    pub fn into_changes(self) -> Vec<ChangeRecord> {
        self.logs
    }

    fn contains(&self, peer_id: u64) -> bool {
        match self.logs.iter().rfind(|ChangeRecord(id, _)| {*id == peer_id}) {
            Some(ChangeRecord(_, ChangeType::AddPeer)) => true,
            Some(ChangeRecord(_, ChangeType::RemovePeer)) => false,
            None => self.base.contains_key(&peer_id)
        }
    }
}

pub struct ClusterChanger<'a> {
    tracker: &'a ProgressTracker
}

impl ClusterChanger<'_> {

    pub fn new(tracker: &ProgressTracker) -> ClusterChanger {
        ClusterChanger { tracker }
    }
    
    pub fn restore(tracker: &mut ProgressTracker, next_index: u64, config_changes: &ConfState) -> Result<()> {
        let (incoming_confs, outgoing_confs) = Self::read_changes_from_config(config_changes);

        let apply_confs = |confs: Vec<ConfChange>, tracker: &mut ProgressTracker| -> Result<()> {
            for change in confs {
                let (cluster, changes) = ClusterChanger::new(tracker).simple(&[change])?;
                tracker.apply_cluster_changes(cluster, changes, next_index);
            }
            Ok(())
        };

        if outgoing_confs.is_empty() {
            apply_confs(incoming_confs, tracker)?;
        } else {
            apply_confs(outgoing_confs, tracker)?;
            let (cluster, changes) = ClusterChanger::new(tracker).enter_joint(&incoming_confs, config_changes.auto_leave)?;
            tracker.apply_cluster_changes(cluster, changes, next_index);
        }
        Ok(())
    }

    /// handle changes in "simple" way, without update joint incoming and outgoting,
    /// just apply changes and update conf_state.
    pub fn simple(&mut self, changes: &[ConfChange]) -> Result<(Cluster, Vec<ChangeRecord>)> {
        if self.tracker.quorum.already_joint() {
            return Err(Error::ConfChange(
                "can't apply simple config change in joint config".to_owned(),
            ));
        }

        let (mut ori_cluster, mut change_records) = self.make_change_log()?;
        self.apply(&mut ori_cluster, &mut change_records, changes)?;
        
        if self.detect_changes(&ori_cluster) > 1 {
            return Err(Error::ConfChange(
                "more than one voter changed without entering joint config".to_owned(),
            ));
        }
        check_invariants(&ori_cluster, &change_records)?;
        Ok((ori_cluster, change_records.into_changes()))
    }

    #[inline]
    pub fn detect_changes(&self, origin_cluster: &Cluster) -> usize {
        Self::detect_changes_between(origin_cluster, &self.tracker)
    }

    #[inline]
    pub fn detect_changes_between(origin_cluster: &Cluster, latest_cluster: &Cluster) -> usize {
        origin_cluster.incoming().symmetric_difference(latest_cluster.incoming()).count()
    }

    pub fn enter_joint(&self, changes: &[ConfChange], auto_leave: bool) -> Result<(Cluster, Vec<ChangeRecord>)> {
        if self.tracker.quorum.already_joint() {
            return Err(Error::ConfChange(String::from("cluster is already joint")));
        }

        let (mut cluster, mut change_log) = self.make_change_log()?;
        let (incoming, outgoing) = (&cluster.quorum.incoming, &mut cluster.quorum.outgoing);
        if incoming.is_empty() {
            return Err(Error::ConfChange(String::from("should have at least one voter in the joint")))
        }
        // before add node to incoming, the old incoming will be extend to outgoing.
        // e.g. old incoming is [1,2,3], outgoting is empty, attempt to add 4 remove 1, 
        // before that, change outgoing to [1,2,3], and apply changes to incoming, then 
        // incoming is [2,3,4]
        outgoing.extend(incoming.iter().cloned());
        self.apply(&mut cluster, &mut change_log, changes)?;
        cluster.auto_leave = auto_leave;
        check_invariants(&cluster, &change_log)?;
        Ok((cluster, change_log.into_changes()))
    }

    pub fn leave_joint(&mut self) -> Result<(Cluster, Vec<ChangeRecord>)> {
        if !self.tracker.quorum.already_joint() {
            return Err(Error::ConfChange(String::from("can't leave a non-joint cluster")));
        }
        let (mut cluster, mut change_log) = self.make_change_log()?;
        let (learners, incoming, outgoing) = (
            &mut cluster.learners, 
            &cluster.quorum.incoming, 
            &mut cluster.quorum.outgoing
        );

        if outgoing.is_empty() {
            return Err(Error::ConfChange(format!("cluster is not joint: {:?}", cluster)));
        }
        learners.extend(cluster.learners_next.drain());
        for leave_peer in outgoing.iter() {
            if !incoming.contains(leave_peer) && !learners.contains(leave_peer) {
                change_log.append_change(*leave_peer, ChangeType::RemovePeer);
            }
        }
        outgoing.clear();
        cluster.auto_leave = false;
        check_invariants(&cluster, &change_log)?;
        Ok((cluster, change_log.into_changes()))
    }

    /// Applies a change to the configuration. By convention, changes to voters are always
    /// made to the ***quorum1*** majority. ***quorum2*** is either empty or preserves the
    /// outgoing majority configuration while in a joint state.
    fn apply(&self, origin_cluster: &mut Cluster, change_records: &mut ChangeRecords, changes: &[ConfChange]) -> Result<()> {
        for change in changes {
            let (node_id, change_type) = (change.node_id, change.change_type());
            if node_id == DUMMY_ID {
                continue;
            }
            if !change_records.contains(node_id) {
                if change_type != RemoveNode {
                    self.init_change_log(origin_cluster, change_records, node_id, change_type == AddLearnerNode)
                }
                continue;
            }

            match change_type {
                AddNode => {
                    origin_cluster.make_voter(node_id);
                },
                AddLearnerNode => {
                    origin_cluster.make_learner(node_id);
                },
                RemoveNode => {
                    let still_voter = origin_cluster.remove(node_id);
                    // If the peer is still a voter in the outgoing config, keep the Progress.
                    if !still_voter {
                        change_records.append_change(node_id, ChangeType::RemovePeer);
                    }
                }
            }
        }

        if origin_cluster.incoming().is_empty() {
            return Err(Error::ConfChange("removed all voters".to_owned()));
        }
        Ok(())
    }

    fn init_change_log(&self, cluster: &mut Cluster, change_log: &mut ChangeRecords, for_peer: u64, is_learner: bool) {
        if !is_learner {
            cluster.incoming_mut().insert(for_peer);
        } else {
            cluster.learners.insert(for_peer);
        }
        change_log.append_change(for_peer, ChangeType::AddPeer);
    }

    /// Get the snapshot from current cluster and create an 
    /// incremental ChangeRecords.
    /// ### Returns 
    /// **Cluster**: copy of cluster (includes voters, learners and incoming outgoing).
    /// **ChangeRecords**: could be used to record changes of cluster.
    fn make_change_log(&self) -> Result<(Cluster, ChangeRecords)> {
        let change_log = ChangeRecords {
            base: self.tracker.all_progress(),
            logs: vec![]
        };
        check_invariants(self.tracker.cluster_info(), &change_log)?;
        Ok((self.tracker.cluster_info().clone(), change_log))
    }

    fn read_changes_from_config(config_changes: &ConfState) -> (Vec<ConfChange>, Vec<ConfChange>) {
        let mut incoming_confs = Vec::new();
        let mut outgoing_confs = Vec::new();
        for &leave_voter in config_changes.voters_outgoing.iter() {
            outgoing_confs.push(ConfChange::new(leave_voter, AddNode));
            incoming_confs.push(ConfChange::new(leave_voter, RemoveNode));
        }

        for &enter_voter in config_changes.voters.iter() {
            incoming_confs.push(ConfChange::new(enter_voter, AddNode));
        }

        for &enter_learner in config_changes.learners.iter() {
            incoming_confs.push(ConfChange::new(enter_learner, AddLearnerNode));
        }

        for &enter_pre_learner in config_changes.learners_next.iter() {
            incoming_confs.push(ConfChange::new(enter_pre_learner, AddLearnerNode));
        }
        
        (incoming_confs, outgoing_confs)
    }
}

fn check_invariants(cluster: &Cluster, change_log: &ChangeRecords) -> Result<()> {
    // NB: intentionally allow the empty config. In production we'll never see a
    // non-empty config (we prevent it from being created) but we will need to
    // be able to *create* an initial config, for example during bootstrap (or
    // during tests). Instead of having to hand-code this, we allow
    // transitioning from an empty config into any other legal and non-empty
    // config.
    for id in cluster.quorum.ids() {
        if !change_log.contains(id) {
            return Err(Error::ConfChange(format!(
                "no progress for voter {}",
                id
            )));
        }
    }
    for id in &cluster.learners {
        if !change_log.contains(*id) {
            return Err(Error::ConfChange(format!(
                "no progress for learner {}",
                id
            )));
        }
        // Conversely Learners and Voters doesn't intersect at all.
        if cluster.outgoing().contains(id) {
            return Err(Error::ConfChange(format!(
                "{} is in learners and outgoing voters",
                id
            )));
        }
        if cluster.incoming().contains(id) {
            return Err(Error::ConfChange(format!(
                "{} is in learners and incoming voters",
                id
            )));
        }
    }
    for id in &cluster.learners_next {
        if !change_log.contains(*id) {
            return Err(Error::ConfChange(format!(
                "no progress for learner(next) {}",
                id
            )));
        }

        // Any staged learner was staged because it could not be directly added due
        // to a conflicting voter in the outgoing config.
        if !cluster.outgoing().contains(id) {
            return Err(Error::ConfChange(format!(
                "{} is in learners_next and outgoing voters",
                id
            )));
        }
    }

    if !cluster.quorum.already_joint() {
        // Etcd enforces outgoing and learner_next to be nil map. But there is no nil
        // in rust. We just check empty for simplicity.
        if !cluster.learners_next.is_empty() {
            return Err(Error::ConfChange(
                "learners_next must be empty when not joint".to_owned(),
            ));
        }
        if cluster.auto_leave {
            return Err(Error::ConfChange(
                "auto_leave must be false when not joint".to_owned(),
            ));
        }
    }

    Ok(())
}

#[test]
pub fn test_change_log() {

    let tracker = ProgressTracker::empty(10);
    let mut logger = ChangeRecords::new(&tracker);
    logger.append_change(1, ChangeType::AddPeer);
    logger.append_change(1, ChangeType::RemovePeer);
    logger.append_change(2, ChangeType::AddPeer);
    let contained_1 = logger.contains(1);
    let contained_2 = logger.contains(2);
    assert!(!contained_1);
    assert!(contained_2);
}
