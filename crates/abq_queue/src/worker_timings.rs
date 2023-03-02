//! Utilities for analyzing worker timings relative to a test suite run.

use std::time;

use abq_utils::{
    net_protocol::{entity::Entity, workers::RunId},
    vec_map::VecMap,
};

/// Maps a worker's [entity][EntityId] to a moment in time they connected to the queue.
pub type WorkerTimings = VecMap<Entity, time::Instant>;

pub fn log_workers_waited_for_manifest_latency(
    run_id: &RunId,
    // The times workers first connected to the queue, for the given run ID.
    worker_first_connected_times: VecMap<Entity, time::Instant>,
    manifest_received_time: time::Instant,
) {
    for (worker, worker_connected) in worker_first_connected_times {
        let pre_manifest_idle_seconds = (manifest_received_time - worker_connected).as_secs();
        tracing::info!(
            entity_id=%worker.display_id(),
            entity_tag=%worker.tag,
            %run_id,
            ?pre_manifest_idle_seconds,
            "worker pre-manifest idle seconds"
        );
    }
}
