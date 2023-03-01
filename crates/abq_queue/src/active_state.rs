use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc},
};

use abq_utils::{atomic, net_protocol::workers::RunId};
use parking_lot::RwLock;

/// Cache of the current state of active runs and their result, so that we don't have to
/// lock the run queue to understand what results we are waiting on.
#[derive(Default, Clone)]
pub struct RunStateCache(Arc<RwLock<HashMap<RunId, ActiveRunState>>>);

struct ActiveRunState {
    /// Amount of work items left for the run.
    work_left: AtomicUsize,
}

impl RunStateCache {
    /// Returns `true` if a state was already present.
    #[must_use]
    pub fn insert_new(&self, run_id: RunId, manifest_len: usize) -> bool {
        let mut states = self.0.write();
        let old = states.insert(
            run_id,
            ActiveRunState {
                work_left: AtomicUsize::new(manifest_len),
            },
        );
        old.is_some()
    }

    /// Returns `true` if the state was present.
    #[must_use]
    pub fn remove(&self, run_id: &RunId) -> bool {
        let mut states = self.0.write();
        let old = states.remove(run_id);
        old.is_some()
    }

    /// Returns whether any work is left for the given run.
    /// If the run does not exist, returns `None`.
    #[must_use]
    pub fn account_results(&self, run_id: &RunId, num_results: usize) -> Option<bool> {
        let states = self.0.read();
        let run_state = states.get(run_id)?;

        let ActiveRunState { work_left } = run_state;

        let old_num_results = work_left.fetch_sub(num_results, atomic::ORDERING);

        let num_left = old_num_results - num_results;
        Some(num_left == 0)
    }

    #[must_use]
    pub fn contains(&self, run_id: &RunId) -> bool {
        self.0.read().contains_key(run_id)
    }
}
