use std::{collections::HashMap, sync::Arc, time};

use abq_utils::{
    exit::ExitCode,
    net_protocol::{entity::EntityId, workers::RunId},
    vec_map::VecMap,
};
use parking_lot::Mutex;

use crate::worker_timings::{new_worker_timings, WorkerTimings};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RunSuccess {
    /// So far, has the run been entirely successful?
    Success(bool),
    /// The success and exit code of the run will be determined by an external process.
    DetermineOutOfBand,
}

impl RunSuccess {
    fn account(&mut self, partial_result_successful: bool) {
        match self {
            RunSuccess::Success(success) => {
                *success = *success && partial_result_successful;
            }
            RunSuccess::DetermineOutOfBand => {}
        }
    }

    pub fn into_opt_exit_code(self) -> Option<ExitCode> {
        match self {
            RunSuccess::Success(true) => Some(ExitCode::SUCCESS),
            RunSuccess::Success(false) => Some(ExitCode::FAILURE),
            RunSuccess::DetermineOutOfBand => None,
        }
    }
}

pub struct ActiveRunState {
    /// Amount of work items left for the run.
    work_left: usize,
    run_success: RunSuccess,

    /// The timestamps at which each worker in the test run completed all tests it was assigned to.
    /// This data is stored to estimate deltas between when a worker completed its assigned
    /// test, and when the test run completed overall.
    ///
    /// It is most important to estimate deltas that are larger, rather than smaller.
    // NB: the number of workers is typically very very small (sub-dozens). Keep an associative
    // list to minimize memory pressure and CPU time.
    worker_completed_times: WorkerTimings,
}

impl ActiveRunState {
    pub fn new(manifest_len: usize, starting_run_success_state: RunSuccess) -> Self {
        Self {
            work_left: manifest_len,
            run_success: starting_run_success_state,
            worker_completed_times: new_worker_timings(),
        }
    }

    /// Returns whether any work is left, and the current success status.
    pub fn account_results(
        &mut self,
        num_results: usize,
        is_any_fail_like: bool,
    ) -> (bool, RunSuccess) {
        // TODO: hedge against underflow here
        self.work_left -= num_results;

        self.run_success.account(!is_any_fail_like);

        (self.work_left == 0, self.run_success)
    }

    pub fn insert_worker_completed(
        &mut self,
        worker: EntityId,
        notification_time: time::Instant,
    ) -> Option<time::Instant> {
        self.worker_completed_times
            .insert(worker, notification_time)
    }

    pub fn worker_completed_times(self) -> VecMap<EntityId, time::Instant> {
        self.worker_completed_times
    }
}

/// Cache of the current state of active runs and their result, so that we don't have to
/// lock the run queue to understand what results we are waiting on.
// TODO: consider using DashMap or RwLock<Map<RunId, Mutex<RunResultState>>>
pub type RunStateCache = Arc<Mutex<HashMap<RunId, ActiveRunState>>>;
