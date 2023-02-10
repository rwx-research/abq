//! Utilities for analyzing worker timings relative to a test suite run.

use std::time;

use abq_utils::{
    net_protocol::{entity::Entity, workers::RunId},
    vec_map::VecMap,
};

const ESTIMATED_TYPICAL_NUMBER_OF_WORKERS: usize = 4;

/// Maps a worker's [entity][EntityId] to a moment in time they connected to the queue.
pub type WorkerTimings = VecMap<Entity, time::Instant>;

pub fn new_worker_timings() -> WorkerTimings {
    WorkerTimings::with_capacity(ESTIMATED_TYPICAL_NUMBER_OF_WORKERS)
}

pub fn log_workers_idle_after_completion_latency(
    run_id: &RunId,
    mut worker_completed_times: VecMap<Entity, time::Instant>,
    worker_with_last_result: Entity,
) {
    let run_completion_time = time::Instant::now();
    if !worker_completed_times.contains(&worker_with_last_result) {
        worker_completed_times.insert(worker_with_last_result, run_completion_time);
    }
    for (worker, worker_completion_time) in worker_completed_times {
        let post_completion_idle_seconds = (run_completion_time - worker_completion_time).as_secs();
        tracing::info!(
            ?run_id,
            ?worker,
            ?post_completion_idle_seconds,
            "worker post completion idle seconds"
        );
    }
}

pub fn log_workers_waited_for_manifest_latency(
    run_id: &RunId,
    // The times workers first connected to the queue, for the given run ID.
    worker_first_connected_times: VecMap<Entity, time::Instant>,
    manifest_received_time: time::Instant,
) {
    for (worker, worker_connected) in worker_first_connected_times {
        let pre_manifest_idle_seconds = (manifest_received_time - worker_connected).as_secs();
        tracing::info!(
            ?run_id,
            ?worker,
            ?pre_manifest_idle_seconds,
            "worker pre-manifest idle seconds"
        );
    }
}

pub fn log_workers_waited_for_supervisor_latency(
    run_id: &RunId,
    worker_first_connected_times: &VecMap<Entity, time::Instant>,
    supervisor_started_run_time: time::Instant,
) {
    for (worker, worker_connected) in worker_first_connected_times.iter() {
        let pre_manifest_idle_seconds = (supervisor_started_run_time - *worker_connected).as_secs();
        tracing::info!(
            ?run_id,
            ?worker,
            ?pre_manifest_idle_seconds,
            "worker pre-supervisor idle seconds"
        );
    }
}
