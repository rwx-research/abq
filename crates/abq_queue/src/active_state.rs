use std::{collections::HashMap, sync::Arc};

use abq_utils::net_protocol::workers::RunId;
use parking_lot::Mutex;

pub struct ActiveRunState {
    /// Amount of work items left for the run.
    work_left: usize,
}

impl ActiveRunState {
    pub fn new(manifest_len: usize) -> Self {
        Self {
            work_left: manifest_len,
        }
    }

    /// Returns whether any work is left, and the current success status.
    pub fn account_results(&mut self, num_results: usize) -> bool {
        // TODO: hedge against underflow here
        self.work_left -= num_results;

        self.work_left == 0
    }
}

/// Cache of the current state of active runs and their result, so that we don't have to
/// lock the run queue to understand what results we are waiting on.
// TODO: consider using DashMap or RwLock<Map<RunId, Mutex<RunResultState>>>
pub type RunStateCache = Arc<Mutex<HashMap<RunId, ActiveRunState>>>;
