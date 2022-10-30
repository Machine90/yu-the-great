use std::{collections::HashSet};

use crate::raft_log_proto::ConfState;

impl From<Vec<u64>> for ConfState {
    fn from(voters: Vec<u64>) -> Self {
        ConfState { voters, ..Default::default() }
    }
}

impl From<HashSet<u64>> for ConfState {
    fn from(voters: HashSet<u64>) -> Self {
        let mut voter_vec = Vec::with_capacity(voters.len());
        for v in voters {
            voter_vec.push(v);
        }
        ConfState { voters: voter_vec, ..Default::default() }
    }
}

impl ConfState {
    pub fn get_voters(&self) -> &[u64] {
        &self.voters
    }

    pub fn get_learners(&self) -> &[u64] {
        &self.learners
    }

    pub fn get_voters_outgoing(&self) -> &[u64] {
        &self.voters_outgoing
    }

    /// Clone all voters from confstate, voters include: 
    /// voters and voters_outgoing.
    pub fn all_voters(&self) -> HashSet<u64> {
        let mut voters = HashSet::new();
        voters.extend(self.get_voters());
        voters.extend(self.get_voters_outgoing());
        voters
    }

    pub fn get_learners_next(&self) -> &[u64] {
        &self.learners_next
    }

    pub fn get_auto_leave(&self) -> bool {
        self.auto_leave
    }

    #[must_use]
    pub fn is_conf_state_eq(lhs: &ConfState, rhs: &ConfState) -> bool {
        if lhs.get_voters() == rhs.get_voters()
            && lhs.get_learners() == rhs.get_learners()
            && lhs.get_voters_outgoing() == rhs.get_voters_outgoing()
            && lhs.get_learners_next() == rhs.get_learners_next()
            && lhs.auto_leave == rhs.auto_leave
        {
            return true;
        }
    
        is_eq_without_order(lhs.get_voters(), rhs.get_voters())
            && is_eq_without_order(lhs.get_learners(), rhs.get_learners())
            && is_eq_without_order(lhs.get_voters_outgoing(), rhs.get_voters_outgoing())
            && is_eq_without_order(lhs.get_learners_next(), rhs.get_learners_next())
            && lhs.auto_leave == rhs.auto_leave
    }
}

fn is_eq_without_order(lhs: &[u64], rhs: &[u64]) -> bool {
    for l in lhs {
        if !rhs.contains(l) {
            return false;
        }
    }
    for r in rhs {
        if !lhs.contains(r) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod test {
    use crate::{raft_log_proto::{HardState}};

    #[test] fn test_eq_hs() {
        let mut hs1 = HardState::default();
        hs1.commit = 1;
        hs1.vote = 1;
        hs1.term = 1;

        let mut hs2 = HardState::default();
        hs2.commit = 1;
        hs2.vote = 1;
        hs2.term = 1;

        let mut hs3 = HardState::default();
        hs3.commit = 2;
        hs3.vote = 1;
        hs3.term = 1;

        let mut hs4 = HardState::default();
        hs4.commit = 1;
        hs4.vote = 2;
        hs4.term = 1;

        let mut hs5 = HardState::default();
        hs5.commit = 1;
        hs5.vote = 1;
        hs5.term = 2;

        assert!(hs1 == hs2);
        assert!(hs1 != hs3);
        assert!(hs1 != hs4);
        assert!(hs1 != hs5);
    }
}