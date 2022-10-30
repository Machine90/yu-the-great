use std::{ops::Deref, sync::Arc};

use super::RaftCoprocessor;

pub type Coprocessor = Arc<dyn RaftCoprocessor + Send>;

/// Executor of coprocessor, each executor
/// contain a coprocessor's reference, and it's
/// id & priority. Thus `clone` of `Executor` equal
/// to clone of coprocessor's reference.
#[derive(Clone)]
pub struct Executor {
    coprocessor: Coprocessor,
}

impl Deref for Executor {
    type Target = Coprocessor;

    fn deref(&self) -> &Self::Target {
        &self.coprocessor
    }
}

impl Executor {
    pub fn new(coprocessor: Coprocessor) -> Self {
        Self { coprocessor }
    }
}
