
#[derive(Debug, PartialEq, Clone)]
pub enum ProgressState {
    /// When leader raft starting to probe the log position
    Probe, 
    /// After leader probe the position of the rafts' log, then enter the replicate state
    Replicate, 
    /// When raft process snapshot which received from leader, then enter this state
    Snapshot 
}

impl Default for ProgressState {
    fn default() -> Self {
        ProgressState::Probe
    }
}

impl ToString for ProgressState {
    fn to_string(&self) -> String {
        match &self {
            ProgressState::Probe => "Probe".to_owned(),
            ProgressState::Replicate => "Replicate".to_owned(),
            ProgressState::Snapshot => "Snapshot".to_owned()
        }
    }
}