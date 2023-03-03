use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{io, time};

use abq_utils::auth::ServerAuthStrategy;
use abq_utils::exit::ExitCode;
use abq_utils::net_async::{self, UnverifiedServerStream};
use abq_utils::net_opt::ServerOptions;
use abq_utils::net_protocol::entity::Entity;
use abq_utils::net_protocol::queue::{
    AssociatedTestResults, CancelReason, NativeRunnerInfo, NegotiatorInfo, Request, TestSpec,
};
use abq_utils::net_protocol::runners::{CapturedOutput, MetadataMap};
use abq_utils::net_protocol::work_server::{self, RetryManifestResponse};
use abq_utils::net_protocol::workers::{
    ManifestResult, NextWorkBundle, ReportedManifest, WorkerTest, INIT_RUN_NUMBER,
};
use abq_utils::net_protocol::{
    self,
    queue::{InvokeWork, Message, RunStatus},
    workers::{NextWork, RunId},
};
use abq_utils::net_protocol::{meta, publicize_addr};
use abq_utils::server_shutdown::{ShutdownManager, ShutdownReceiver};
use abq_utils::tls::ServerTlsStrategy;
use abq_utils::vec_map::VecMap;
use abq_utils::{atomic, log_assert};
use abq_workers::negotiate::{
    AssignedRun, AssignedRunStatus, QueueNegotiator, QueueNegotiatorHandle,
    QueueNegotiatorServerError,
};

use parking_lot::{Mutex, RwLock, RwLockWriteGuard};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;
use tracing::instrument;

use crate::job_queue::JobQueue;
use crate::persistence::manifest::{
    self, ManifestPersistedCell, PersistManifestPlan, SharedPersistManifest,
};
use crate::prelude::*;
use crate::worker_timings::{log_workers_waited_for_manifest_latency, WorkerTimings};
use crate::worker_tracking::WorkerSet;

pub const DEFAULT_CLIENT_POLL_TIMEOUT: Duration = Duration::from_secs(60 * 60);

#[derive(Debug)]
enum RunState {
    /// First worker has connected. Waiting for manifest.
    WaitingForManifest {
        /// For the purposes of analytics, records timings of when workers connect prior to the
        /// manifest being generated.
        worker_connection_times: Mutex<WorkerSet<Instant>>,
    },
    /// The active state of the test suite run. The queue is populated and at least one worker is
    /// connected.
    HasWork {
        queue: JobQueue,

        /// Top-level test suite metadata workers should initialize with.
        init_metadata: MetadataMap,

        /// The number of tests to batch to a worker at a time, as hinted by an invoker of the work.
        // Co-locate this here so that we don't have to index `run_data` when grabbing a
        // test.
        batch_size_hint: NonZeroUsize,

        /// Workers that have connected to execute tests, and whether they are still executing
        /// tests.
        /// Some(time) if they have already completed, None if they are still active.
        active_workers: Mutex<WorkerSet<Option<Instant>>>,
    },
    /// All items in the manifest have been handed out.
    /// Workers may still be executing locally, for example in-band retries.
    InitialManifestDone {
        /// The exit code for the test run that should be yielded by a new worker connecting.
        new_worker_exit_code: ExitCode,

        /// Top-level test suite metadata workers should initialize with.
        /// Must be persisted for out-of-process retries.
        init_metadata: MetadataMap,

        /// Workers that have been seen for this run, and whether they are still active.
        /// Any seen worker is elligible for out-of-process retries.
        seen_workers: RwLock<WorkerSet<bool>>,

        /// A marker of whether the manifest associated with this run has been persisted.
        manifest_persistence: ManifestPersistence,
    },
    Cancelled {
        #[allow(unused)] // yet
        reason: CancelReason,
    },
}

#[derive(Debug)]
enum ManifestPersistence {
    Persisted(ManifestPersistedCell),
    ManifestNeverReceived,
    EmptyManifest,
}

struct RunData {
    /// The number of tests to batch to a worker at a time, as hinted by an invoker of the work.
    batch_size_hint: NonZeroUsize,
}

static_assertions::assert_eq_size!(Option<RunData>, RunData);

const MAX_BATCH_SIZE: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(100) };

/// A individual test run ever invoked on the queue.
struct Run {
    /// The state of the test run.
    state: RunState,
    /// Data for the test run, persisted only while a test run is enqueued or active.
    /// One a run is done (or cancelled), its state is persisted, but its run data is dropped to
    /// minimize memory leaks.
    data: Option<RunData>,
}

/// Sync-safe storage of all runs ever invoked on the queue.
#[derive(Default)]
struct AllRuns {
    /// Shared runs are represented as a two-tiered read/write lock.
    ///
    /// RwLock => Map<RunId, // layer 1
    ///     RwLock => [Run]> // layer 2
    ///
    ///   Layer 1: read/write lock to insert and access runs. Insertion of a new run happens only once
    ///      per run, but access of run state is very common.
    ///   Layer 2: read/write lock to access the run information.
    ///      We only need to write to the run when it's transitioning between states - for example,
    ///      into [RunState::HasWork] or [RunState::Cancelled].
    ///      The hottest regions of operations are those over [RunState::HasWork], for which we
    ///      want minimal synchronization. [RunState::HasWork] is organized to only require minimal
    ///      atomic synchronization when accessing work; see its description and [JobQueue] for details.
    runs: RwLock<
        // layer1
        HashMap<
            RunId,
            // layer2
            RwLock<Run>,
        >,
    >,
    /// An intentionally-conservative estimation of the number of active runs, possibly
    /// over-estimating the number of active runs. The exact behavior is as follows:
    ///
    ///   - When a run is added, the number of active runs may be reflected to include that new
    ///     run before all other threads see the run in the `runs` map.
    ///   - When a run is removed, the number of active runs may NOT be reflected to exclude that
    ///     run before all other threads see the run removed from the `runs` map.
    ///
    /// This is done because `num_active` should only be used as an estimation for determining
    /// whether a queue should be shutdown anyway, since new run requests may come in any time.
    /// Since we are okay with estimates in these cases, we can avoid taking locks in adjusting
    /// the number of active runs in tandem with the `runs` map.
    num_active: AtomicU64,
}

type RunsWriteGuard<'a> = RwLockWriteGuard<'a, HashMap<RunId, RwLock<Run>>>;

#[derive(Default, Clone)]
struct SharedRuns(Arc<AllRuns>);

impl Deref for SharedRuns {
    type Target = AllRuns;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

enum InitMetadata {
    Metadata(MetadataMap),
    WaitingForManifest,
    RunAlreadyCompleted { cancelled: bool },
}

/// What is the state of the queue of tests for a run after some bundle of tests have been pulled?
#[derive(Clone, Copy, Debug)]
enum PulledTestsStatus {
    /// There are additional tests remaining.
    MoreTestsRemaining,
    /// The bundle of tests just pulled have exhausted the queue of tests.
    /// The last test to be handed out is in this bundle.
    PulledLastTest,
    /// The queue was already empty when tests were pulled.
    QueueWasEmpty,
}

impl PulledTestsStatus {
    fn reached_end_of_tests(&self) -> bool {
        match self {
            PulledTestsStatus::MoreTestsRemaining => false,

            PulledTestsStatus::PulledLastTest { .. } => true,
            PulledTestsStatus::QueueWasEmpty => true,
        }
    }
}

/// Result of trying to pull a batch of tests for a worker.
struct NextWorkResult<'a> {
    bundle: NextWorkBundle,
    status: PulledTestsStatus,
    opt_persistent_manifest_plan: Option<PersistManifestPlan<'a>>,
}

#[derive(Debug)]
#[must_use]
enum AddedManifest {
    Added {
        /// For the purposes of analytics, timings of when workers first connected
        /// prior to the manifest being generated.
        worker_connection_times: WorkerTimings,
    },
    RunCancelled,
}

enum RetryManifestState {
    Manifest(NextWorkBundle),
    RunNotFound,
    ManifestNeverReceived,
    NotYetPersisted,
    FetchFromPersistence,
}

impl AllRuns {
    /// Finds a queue for a run, or creates a new one if the run is observed as fresh.
    /// If the given run ID already has an associated queue, an error is returned.
    #[must_use]
    pub fn find_or_create_run(
        &self,
        run_id: &RunId,
        batch_size_hint: NonZeroUsize,
        entity: Entity,
    ) -> AssignedRunStatus {
        {
            if self.runs.read().get(run_id).is_none() {
                let runs = self.runs.write();

                // Possible TOCTOU race here, so we must check whether the run is still missing
                // before we create a fresh queue.
                //
                // If the run state is now populated, defer to that, since it can only
                // monotonically advance the state machine (run states move forward and are
                // never removed).
                if !runs.contains_key(run_id) {
                    return Self::create_fresh_run(
                        runs,
                        &self.num_active,
                        entity,
                        run_id.clone(),
                        batch_size_hint,
                    );
                }
            };
        }

        let runs = self.runs.read();
        let run = match runs.get(run_id) {
            Some(st) => st.read(),
            None => {
                tracing::error!(
                    ?run_id,
                    "illegal state - a run must always be populated when looking up for worker"
                );
                return AssignedRunStatus::RunUnknown;
            }
        };

        match &run.state {
            RunState::WaitingForManifest {
                worker_connection_times,
            } => {
                let mut worker_connection_times = worker_connection_times.lock();
                let old = worker_connection_times.insert_by_tag(entity, time::Instant::now());
                log_assert!(
                    old.is_none(),
                    ?entity,
                    "same worker connecting twice for manifest"
                );

                AssignedRunStatus::Run(AssignedRun::Fresh {
                    should_generate_manifest: false,
                })
            }
            RunState::HasWork { active_workers, .. } => {
                let mut active_workers = active_workers.lock();
                let old_active_worker_info = active_workers.insert_by_tag(entity, None);

                match old_active_worker_info {
                    None => {
                        // This is a fresh worker.
                        AssignedRunStatus::Run(AssignedRun::Fresh {
                            should_generate_manifest: false,
                        })
                    }
                    Some((old_entity, old_finished_state)) => {
                        if old_entity.id == entity.id {
                            // The same worker entity is connecting twice - this implies that the
                            // same worker process is asking to find a run more than once, which
                            // should never happen. Each worker process should be associated with
                            // exactly one run.
                            log_assert!(
                                false,
                                ?entity,
                                ?run_id,
                                "same worker entity connecting to find run more than once"
                            );
                        }

                        // The entities are different, but their tags are the same. As such, this
                        // is a worker re-connecting out-of-process, presumably for a retry.
                        //
                        // Unfortunately, we cannot reliably guard on `old_finished_state`
                        // indicating that the worker sent a finished-all-results notification
                        // here. That's because the worker may have spuriously crashed, and failed
                        // to notify us of its failure.
                        //
                        // As such, we don't attempt to distinguish between duplicate workers and
                        // out-of-process-retrying workers at this time, and instead always provide
                        // the subset of the manifest the worker was previously assigned to run.
                        //
                        // TODO(out-of-process-retries): right now this query happens on the level
                        // of a worker pool. Ideally, each runner would query for find_or_create_run
                        // itself, and if this is done we could hand out the manifest right here,
                        // rather than asking the runner to reconnect to retrieve the manifest.
                        tracing::info!(
                            ?old_finished_state,
                            ?entity,
                            "worker reconnecting for out-of-process retry manifest"
                        );

                        AssignedRunStatus::Run(AssignedRun::Retry)
                    }
                }
            }
            RunState::InitialManifestDone {
                new_worker_exit_code,
                seen_workers,
                ..
            } => {
                log_assert!(run.data.is_none(), "run data exists after completed run");

                if seen_workers.read().contains_by_tag(&entity) {
                    // This worker was already involved in this run; assume it's connecting for an
                    // out-of-process retry.
                    AssignedRunStatus::Run(AssignedRun::Retry)
                } else {
                    let exit_code = *new_worker_exit_code;
                    // Fresh workers shouldn't get any new work.
                    // The worker should exit successfully locally, regardless of what the overall
                    // code was.
                    AssignedRunStatus::AlreadyDone { exit_code }
                }
            }
            RunState::Cancelled { .. } => {
                log_assert!(run.data.is_none(), "run data exists after cancelled run");

                AssignedRunStatus::AlreadyDone {
                    exit_code: ExitCode::CANCELLED,
                }
            }
        }
    }

    #[must_use]
    fn create_fresh_run(
        mut runs: RunsWriteGuard<'_>,
        num_active: &AtomicU64,
        worker_entity: Entity,
        run_id: RunId,
        batch_size_hint: NonZeroUsize,
    ) -> AssignedRunStatus {
        let mut worker_timings = WorkerSet::default();
        worker_timings.insert_by_tag(worker_entity, time::Instant::now());

        // NB: Always add first for conversative estimation.
        num_active.fetch_add(1, atomic::ORDERING);

        // The run ID is fresh; create a new queue for it.
        let run = Run {
            state: RunState::WaitingForManifest {
                worker_connection_times: Mutex::new(worker_timings),
            },
            data: Some(RunData { batch_size_hint }),
        };
        let old_run = runs.insert(run_id, RwLock::new(run));
        log_assert!(old_run.is_none(), "can only be called when run is fresh!");

        AssignedRunStatus::Run(AssignedRun::Fresh {
            should_generate_manifest: true,
        })
    }

    /// Adds the initial manifest for an ABQ test suite run.
    pub fn add_manifest(
        &self,
        run_id: &RunId,
        flat_manifest: Vec<TestSpec>,
        init_metadata: MetadataMap,
    ) -> AddedManifest {
        let runs = self.runs.read();

        let mut run = runs.get(run_id).expect("no run recorded").write();

        let worker_connection_times = match &mut run.state {
            RunState::WaitingForManifest {
                worker_connection_times,
            } => {
                // expected state, pass through
                Mutex::into_inner(std::mem::take(worker_connection_times))
            }
            RunState::HasWork { .. } | RunState::InitialManifestDone { .. } => {
                unreachable!(
                    "Invalid state - can only provide manifest while waiting for manifest"
                );
            }
            RunState::Cancelled { .. } => {
                // If cancelled, do nothing.
                return AddedManifest::RunCancelled;
            }
        };

        let work_from_manifest = flat_manifest.into_iter().map(|spec| WorkerTest {
            spec,
            run_number: INIT_RUN_NUMBER,
        });

        let queue = JobQueue::new(work_from_manifest.collect());

        let run_data = run
            .data
            .as_ref()
            .expect("illegal state - run data must exist at this step");

        let mut active_workers = WorkerSet::with_capacity(worker_connection_times.len());
        let mut worker_conn_times = VecMap::with_capacity(worker_connection_times.len());
        for (worker, time) in worker_connection_times {
            active_workers.insert_by_tag(worker, None);
            worker_conn_times.insert(worker, time);
        }

        run.state = RunState::HasWork {
            queue,
            batch_size_hint: run_data.batch_size_hint,
            init_metadata,
            active_workers: Mutex::new(active_workers),
        };

        AddedManifest::Added {
            worker_connection_times: worker_conn_times,
        }
    }

    pub fn init_metadata(&self, run_id: &RunId, entity: Entity) -> InitMetadata {
        let runs = self.runs.read();

        let run = runs.get(run_id).expect("no run recorded").read();

        match &run.state {
            RunState::WaitingForManifest { .. } => InitMetadata::WaitingForManifest,
            RunState::HasWork { init_metadata, .. } => {
                InitMetadata::Metadata(init_metadata.clone())
            }
            RunState::InitialManifestDone {
                init_metadata,
                seen_workers,
                ..
            } => {
                if seen_workers.read().contains_by_tag(&entity) {
                    // This worker was already involved in this run; assume it's connecting for an
                    // out-of-process retry.
                    InitMetadata::Metadata(init_metadata.clone())
                } else {
                    // Fresh workers shouldn't get any new work.
                    // The worker should exit successfully locally, regardless of what the overall
                    // code was.
                    InitMetadata::RunAlreadyCompleted { cancelled: false }
                }
            }
            RunState::Cancelled { .. } => InitMetadata::RunAlreadyCompleted { cancelled: true },
        }
    }

    pub fn next_work<'a>(&self, entity: Entity, run_id: &'a RunId) -> NextWorkResult<'a> {
        let runs = self.runs.read();

        let run = runs.get(run_id).expect("no run recorded").read();

        match &run.state
        {
            RunState::HasWork {
                queue,
                batch_size_hint,
                active_workers,
                ..
            } => {
                active_workers.lock().insert_by_tag_if_missing(entity, None);
                let (bundle, status) = Self::get_next_work_online(queue, entity, *batch_size_hint);
                match status {
                    PulledTestsStatus::MoreTestsRemaining => {
                        NextWorkResult {
                            bundle, status, opt_persistent_manifest_plan: None
                        }
                    }
                    PulledTestsStatus::PulledLastTest { .. } | PulledTestsStatus::QueueWasEmpty => {
                        drop(run);
                        drop(runs);
                        tracing::debug!(?entity, ?run_id, "saw end of manifest for entity");

                        let opt_persistent_manifest_plan = self.try_mark_reached_end_of_manifest(run_id, ExitCode::SUCCESS);

                        NextWorkResult{
                            bundle, status, opt_persistent_manifest_plan
                        }
                    }
                }
            }
            RunState::InitialManifestDone {  seen_workers, .. } => {
                // Let the worker know that we've reached the end of the queue.
                seen_workers.write().insert_by_tag_if_missing(entity, true);
                NextWorkResult{
                    bundle: NextWorkBundle::new(vec![NextWork::EndOfWork]),
                    status: PulledTestsStatus::QueueWasEmpty,
                    opt_persistent_manifest_plan: None,
                }
            }
            RunState::Cancelled {..} => {
                NextWorkResult{
                    bundle: NextWorkBundle::new(vec![NextWork::EndOfWork]),
                    status: PulledTestsStatus::QueueWasEmpty,
                    opt_persistent_manifest_plan: None,
                }
            }
            RunState::WaitingForManifest {..} => unreachable!("Invalid state - work can only be requested after initialization metadata, at which point the manifest is known.")
        }
    }

    fn get_next_work_online(
        queue: &JobQueue,
        entity: Entity,
        batch_size_hint: NonZeroUsize,
    ) -> (NextWorkBundle, PulledTestsStatus) {
        // NB: in the future, the batch size is likely to be determined intelligently, i.e.
        // from off-line timing data. But for now, we use the hint the client provided.
        let batch_size = batch_size_hint;

        // Pop the next batch.
        // TODO: can we get rid of the clone allocation here?
        let mut bundle = Vec::with_capacity(batch_size.get() as _);
        bundle.extend(
            queue
                .get_work(entity.tag, batch_size)
                .cloned()
                .map(NextWork::Work),
        );

        let pulled_tests_status;

        if queue.is_at_end() {
            // If the queue is empty, there are one of the following states:
            //   - We just popped the last test for the last test run attempt. Account for
            //     the latest test run attempt being resolved.
            //
            //   - The end of the queue for the last test run attempt was already reached.
            //     We have nothing to give to the requester other than the end-of-tests
            //     marker.
            //
            //   - There is another upcoming test run attempt, but we don't yet know what
            //     tests compose it. In this case, we must tell the requester to yield
            //     until those results arrive.
            match bundle.last() {
                Some(NextWork::Work(_)) => {
                    pulled_tests_status = PulledTestsStatus::PulledLastTest;
                }
                Some(NextWork::EndOfWork) => {
                    // To avoid additional allocation and moves when we pop the queue, we built
                    // `bundle` as NextWork above, but it only consists of `NextWork::Work` variants.
                    unreachable!("illegal state - EndOfWork variant must not exist at this point.")
                }
                None => {
                    pulled_tests_status = PulledTestsStatus::QueueWasEmpty;
                }
            }
        } else {
            // Queue is not empty.
            pulled_tests_status = PulledTestsStatus::MoreTestsRemaining;
        };

        if pulled_tests_status.reached_end_of_tests() {
            // Let the worker know this is the end, so they don't ask again.
            bundle.push(NextWork::EndOfWork);
        }

        let bundle = NextWorkBundle { work: bundle };

        (bundle, pulled_tests_status)
    }

    pub fn get_retry_manifest(&self, run_id: &RunId, entity: Entity) -> RetryManifestState {
        let runs = self.runs.read();
        let run = match runs.get(run_id) {
            Some(run) => run.read(),
            None => return RetryManifestState::RunNotFound,
        };

        match &run.state {
            RunState::WaitingForManifest { .. } => {
                tracing::error!(
                    ?run_id,
                    ?entity,
                    "illegal state - attempting to fetch retry manifest while waiting for manifest"
                );
                RetryManifestState::RunNotFound
            }
            RunState::HasWork { queue, .. } => {
                // TODO: can we get rid of the clone allocation here?
                //
                // TODO: should we launch discovery of a partition on a blocking async task?
                // If this ever takes a non-trivial amount of time, consider doing so.
                let mut manifest: Vec<_> = queue
                    .get_partition_for_entity(entity.tag)
                    .cloned()
                    .map(NextWork::Work)
                    .collect();
                manifest.push(NextWork::EndOfWork);

                RetryManifestState::Manifest(NextWorkBundle { work: manifest })
            }
            RunState::InitialManifestDone {
                manifest_persistence,
                ..
            } => match manifest_persistence {
                ManifestPersistence::Persisted(cell) => {
                    if cell.is_persisted() {
                        RetryManifestState::FetchFromPersistence
                    } else {
                        RetryManifestState::NotYetPersisted
                    }
                }
                ManifestPersistence::ManifestNeverReceived => {
                    RetryManifestState::ManifestNeverReceived
                }
                ManifestPersistence::EmptyManifest => {
                    RetryManifestState::Manifest(NextWorkBundle {
                        work: vec![NextWork::EndOfWork],
                    })
                }
            },
            RunState::Cancelled { .. } => {
                todo!("fetch retry manifest after cancellation")
            }
        }
    }

    pub fn mark_worker_complete(&self, run_id: &RunId, entity: Entity, notification_time: Instant) {
        let runs = self.runs.read();

        let run = match runs.get(run_id) {
            Some(run) => run.read(),
            None => {
                log_assert!(
                    false,
                    ?entity,
                    "should never mark completed worker for non-existent run"
                );
                return;
            }
        };

        match &run.state {
            RunState::WaitingForManifest { .. } => {
                log_assert!(
                    false,
                    ?entity,
                    "should never mark completed worker while still waiting for manifest"
                );
            }
            RunState::HasWork { active_workers, .. } => {
                let mut active_workers = active_workers.lock();
                let old = active_workers.insert_by_tag(entity, Some(notification_time));
                log_assert!(
                    matches!(old, Some((_, None))),
                    ?entity,
                    ?old,
                    "worker was not seen as active before completion notification"
                );
            }
            RunState::InitialManifestDone { seen_workers, .. } => {
                let mut seen_workers = seen_workers.write();
                let was_present = seen_workers.insert_by_tag(entity, false);
                log_assert!(
                    was_present.is_some(),
                    ?entity,
                    "worker was not seen as active before completion notification",
                );
            }
            RunState::Cancelled { .. } => {
                // cancelled, nothing to do
            }
        }
    }

    fn try_mark_reached_end_of_manifest<'a>(
        &self,
        run_id: &'a RunId,
        new_worker_exit_code: ExitCode,
    ) -> Option<PersistManifestPlan<'a>> {
        let runs = self.runs.read();

        let mut run = runs.get(run_id).expect("no run recorded").write();

        let active_worker_timings;
        let queue;
        let init_metadata;
        match &mut run.state {
            RunState::HasWork {
                queue: this_queue,
                active_workers: this_active_workers,
                init_metadata: this_init_metadata,
                ..
            } => {
                log_assert!(
                    this_queue.is_at_end(),
                    "Invalid state - queue is not complete!"
                );
                active_worker_timings = Mutex::into_inner(std::mem::take(this_active_workers));
                queue = std::mem::take(this_queue);
                init_metadata = std::mem::take(this_init_metadata);
            }
            RunState::Cancelled { .. } => {
                // Cancellation always takes priority over completeness.
                return None;
            }
            RunState::InitialManifestDone { .. } => {
                // We already got marked as completed; bail out.
                return None;
            }
            RunState::WaitingForManifest { .. } => {
                unreachable!("Invalid state");
            }
        };

        tracing::info!(?run_id, "marking end of manifest");

        let mut seen_workers = WorkerSet::default();

        for (worker, opt_completed) in active_worker_timings {
            // The worker is still active if we don't have a completion time for it.
            let is_active = opt_completed.is_none();
            seen_workers.insert_by_tag(worker, is_active);
        }

        // Build the plan to persist the manifest.
        let view = queue.into_manifest_view();
        let (manifest_persisted, plan) = manifest::build_persistence_plan(run_id, view);

        run.state = RunState::InitialManifestDone {
            new_worker_exit_code,
            init_metadata,
            seen_workers: RwLock::new(seen_workers),
            manifest_persistence: ManifestPersistence::Persisted(manifest_persisted),
        };

        // Drop the run data, since we no longer need it.
        log_assert!(run.data.is_some(), "run data missing");
        std::mem::take(&mut run.data);

        // NB: Always sub last for conversative estimation.
        self.num_active.fetch_sub(1, atomic::ORDERING);

        Some(plan)
    }

    pub fn mark_failed_to_receive_manifest(&self, run_id: RunId) {
        let runs = self.runs.read();

        let mut run = runs.get(&run_id).expect("no run recorded").write();

        match run.state {
            RunState::WaitingForManifest { .. } => {
                // okay
            }
            RunState::Cancelled { .. } => {
                // No-op, since the run was already cancelled.
                return;
            }
            RunState::HasWork { .. } | RunState::InitialManifestDone { .. } => {
                unreachable!("Invalid state - can only fail to start while waiting for manifest");
            }
        }

        run.state = RunState::InitialManifestDone {
            new_worker_exit_code: ExitCode::FAILURE,
            init_metadata: Default::default(),
            seen_workers: Default::default(),
            manifest_persistence: ManifestPersistence::ManifestNeverReceived,
        };

        // Drop the run data, since we no longer need it.
        debug_assert!(run.data.is_some());
        std::mem::take(&mut run.data);

        // NB: Always sub last for conversative estimation.
        self.num_active.fetch_sub(1, atomic::ORDERING);
    }

    /// Marks a run as complete because it had the trivial manifest.
    pub fn mark_empty_manifest_complete(&self, run_id: RunId) {
        let runs = self.runs.read();

        let mut run = runs.get(&run_id).expect("no run recorded").write();

        match run.state {
            RunState::WaitingForManifest { .. } => {
                // okay
            }
            RunState::Cancelled { .. } => {
                // No-op, since the run was already cancelled.
                return;
            }
            RunState::HasWork { .. } | RunState::InitialManifestDone { .. } => {
                unreachable!("Invalid state - can only mark complete due to manifest while waiting for manifest");
            }
        }

        // Trivially successful
        run.state = RunState::InitialManifestDone {
            new_worker_exit_code: ExitCode::SUCCESS,
            init_metadata: Default::default(),
            seen_workers: Default::default(),
            manifest_persistence: ManifestPersistence::EmptyManifest,
        };

        // Drop the run data, since we no longer need it.
        debug_assert!(run.data.is_some());
        std::mem::take(&mut run.data);

        // NB: Always sub last for conversative estimation.
        self.num_active.fetch_sub(1, atomic::ORDERING);
    }

    pub fn mark_cancelled(&self, run_id: &RunId, reason: CancelReason) {
        let runs = self.runs.read();

        let mut run = runs.get(run_id).expect("no run recorded").write();

        // Cancellation can happen at any time, including after the test run is determined to have
        // completed by us (the queue).
        //
        // We prefer the observation of the test workers, so test cancellation is always marked
        // as a failure.
        run.state = RunState::Cancelled { reason };

        // Drop the run data if it exists, since we no longer need it.
        // It might already be missing if this cancellation happened after we saw a run complete.
        std::mem::take(&mut run.data);

        // NB: Always sub last for conversative estimation.
        self.num_active.fetch_sub(1, atomic::ORDERING);
    }

    fn get_run_status(&self, run_id: &RunId) -> Option<RunStatus> {
        let runs = self.runs.read();

        let run = runs.get(run_id)?.read();

        match &run.state {
            RunState::WaitingForManifest { .. } | RunState::HasWork { .. } => {
                Some(RunStatus::Active)
            }
            RunState::InitialManifestDone { seen_workers, .. } => {
                let seen_workers = seen_workers.read();
                Some(RunStatus::InitialManifestDone {
                    num_active_workers: seen_workers
                        .iter()
                        .map(|(_, active)| *active as usize)
                        .sum(),
                })
            }
            RunState::Cancelled { .. } => Some(RunStatus::Cancelled),
        }
    }

    #[cfg(test)]
    fn get_active_workers(&self, run_id: &RunId) -> Vec<Entity> {
        let runs = self.runs.read();

        let run = runs.get(run_id).expect("no such run").read();

        match &run.state {
            RunState::WaitingForManifest {
                worker_connection_times,
            } => worker_connection_times.lock().iter().map(|p| p.0).collect(),
            RunState::HasWork { active_workers, .. } => active_workers
                .lock()
                .iter()
                .filter(|p| p.1.is_none())
                .map(|p| p.0)
                .collect(),
            RunState::InitialManifestDone { seen_workers, .. } => seen_workers
                .read()
                .iter()
                .filter(|p| p.1)
                .copied()
                .map(|p| p.0)
                .collect(),
            RunState::Cancelled { .. } => Default::default(),
        }
    }

    fn estimate_num_active_runs(&self) -> u64 {
        self.num_active.load(atomic::ORDERING)
    }

    #[cfg(test)]
    fn set_state(
        &self,
        run_id: RunId,
        state: RunState,
        data: Option<RunData>,
    ) -> Option<RwLock<Run>> {
        let mut runs = self.runs.write();
        runs.insert(run_id, RwLock::new(Run { state, data }))
    }
}

pub struct Abq {
    shutdown_manager: ShutdownManager,

    queues: SharedRuns,

    server_addr: SocketAddr,
    server_handle: Option<JoinHandle<Result<(), QueueServerError>>>,

    work_scheduler_addr: SocketAddr,
    work_scheduler_handle: Option<JoinHandle<Result<(), WorkSchedulerError>>>,

    negotiator: QueueNegotiator,

    active: bool,
}

#[derive(Debug, Error)]
pub enum AbqError {
    #[error("{0}")]
    Queue(#[from] QueueServerError),

    #[error("{0}")]
    WorkScheduler(#[from] WorkSchedulerError),

    #[error("{0}")]
    Negotiator(#[from] QueueNegotiatorServerError),

    #[error("{0}")]
    Io(#[from] io::Error),
}

impl Abq {
    pub fn server_addr(&self) -> SocketAddr {
        self.server_addr
    }

    pub fn work_server_addr(&self) -> SocketAddr {
        self.work_scheduler_addr
    }

    pub async fn start(config: QueueConfig) -> Self {
        start_queue(config).await
    }

    /// Moves the queue into retirement.
    #[instrument(level = "trace", skip(self))]
    pub fn retire(&mut self) {
        self.shutdown_manager.retire()
    }

    /// Moves the queue into retirement.
    #[instrument(level = "trace", skip(self))]
    pub fn is_retired(&self) -> bool {
        self.shutdown_manager.is_retired()
    }

    /// Checks whether the queue has no more active runs.
    /// Guaranteed to be eventually consistent if the queue is [retiring][Self::retire].
    pub fn is_drained(&self) -> bool {
        self.queues.estimate_num_active_runs() == 0
    }

    /// Sends a signal to shutdown immediately.
    #[instrument(level = "trace", skip(self))]
    pub async fn shutdown(&mut self) -> Result<(), AbqError> {
        debug_assert!(self.active);

        self.active = false;

        // Shut down all of our servers.
        self.shutdown_manager.shutdown_immediately()?;

        self.negotiator.join().await;

        self.server_handle
            .take()
            .expect("server handle must be available during shutdown")
            .await
            .unwrap()?;

        self.work_scheduler_handle
            .take()
            .expect("worker handle must be available during shutdown")
            .await
            .unwrap()?;

        Ok(())
    }

    pub fn get_negotiator_handle(&self) -> QueueNegotiatorHandle {
        self.negotiator.get_handle()
    }
}

impl Drop for Abq {
    fn drop(&mut self) {
        if self.active {
            // Our user never called shutdown; try to perform a clean exit.
            // We can't do anything with an error, since this is a drop.
            let _ = self.shutdown();
        }
    }
}

/// Configures initialization of the queue.
pub struct QueueConfig {
    /// The IP address of sockets the queue should advertise, e.g. when negotiating with a worker.
    pub public_ip: IpAddr,
    /// The IP address sockets the queue creates should actually bind to. This can be different
    /// than [public_ip], but is not required to be.
    pub bind_ip: IpAddr,
    /// The port the mainline queue server should bind to. Binds to any port if `0`.
    pub server_port: u16,
    /// The port the work-scheduling server should bind to. Binds to any port if `0`.
    pub work_port: u16,
    /// The port the negotiator server should bind to. Binds to any port if `0`.
    pub negotiator_port: u16,
    /// How the queue should construct its servers.
    pub server_options: ServerOptions,
    /// How manifests should be persisted.
    pub persist_manifest: SharedPersistManifest,
}

impl QueueConfig {
    /// Creates a [`QueueConfig`] that always binds and advertises on INADDR_ANY, with arbitrary
    /// ports for its servers.
    pub fn new(persist_manifest: SharedPersistManifest) -> Self {
        Self {
            public_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            bind_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            server_port: 0,
            work_port: 0,
            negotiator_port: 0,
            server_options: ServerOptions::new(
                ServerAuthStrategy::no_auth(),
                ServerTlsStrategy::no_tls(),
            ),
            persist_manifest,
        }
    }
}

/// Initializes a queue, binding the queue negotiator to the given address.
/// All other public channels to the queue (the work and results server) are exposed
/// on the same host IP address as the negotiator is.
async fn start_queue(config: QueueConfig) -> Abq {
    let QueueConfig {
        public_ip,
        bind_ip,
        server_port,
        work_port,
        negotiator_port,
        server_options,
        persist_manifest,
    } = config;

    let mut shutdown_manager = ShutdownManager::default();

    let queues: SharedRuns = Default::default();

    let server_listener = server_options
        .clone()
        .bind_async((bind_ip, server_port))
        .await
        .unwrap();
    let server_addr = server_listener.local_addr().unwrap();

    let negotiator_listener = server_options
        .clone()
        .bind_async((bind_ip, negotiator_port))
        .await
        .unwrap();
    let negotiator_addr = negotiator_listener.local_addr().unwrap();
    let public_negotiator_addr = publicize_addr(negotiator_addr, public_ip);

    let server_shutdown_rx = shutdown_manager.add_receiver();
    let server_handle = tokio::spawn({
        let queues = queues.clone();
        let queue_server = QueueServer::new(queues, public_negotiator_addr);
        queue_server.start(server_listener, server_shutdown_rx)
    });

    let new_work_server = server_options
        .bind_async((bind_ip, work_port))
        .await
        .unwrap();
    let new_work_server_addr = new_work_server.local_addr().unwrap();

    let work_scheduler_shutdown_rx = shutdown_manager.add_receiver();
    let work_scheduler_handle = tokio::spawn({
        let queues = queues.clone();
        let scheduler = WorkScheduler {
            queues,
            persist_manifest,
        };
        scheduler.start(new_work_server, work_scheduler_shutdown_rx)
    });

    // Provide the execution context a set of workers should attach with, if it is known at the
    // time of polling. We must not to block here, since that can take a lock over the shared
    // queues indefinitely.
    let choose_run_for_worker = {
        let queues = queues.clone();
        move |entity: Entity, invoke_work: &InvokeWork| {
            let InvokeWork {
                run_id,
                batch_size_hint,
                test_results_timeout: _,
            } = invoke_work;

            let batch_size_hint = if batch_size_hint.get() > MAX_BATCH_SIZE.get() as u64 {
                MAX_BATCH_SIZE
            } else {
                NonZeroUsize::new(batch_size_hint.get().try_into().expect(
                    "u64 batch size must fit into a usize batch size less than MAX_BATCH_SIZE",
                ))
                .unwrap()
            };

            queues.find_or_create_run(run_id, batch_size_hint, entity)
        }
    };

    let negotiator_shutdown_rx = shutdown_manager.add_receiver();
    let negotiator = QueueNegotiator::start(
        public_ip,
        negotiator_listener,
        negotiator_shutdown_rx,
        new_work_server_addr,
        server_addr,
        choose_run_for_worker,
    )
    .await;

    Abq {
        shutdown_manager,
        queues,

        server_addr,
        server_handle: Some(server_handle),

        work_scheduler_addr: new_work_server_addr,
        work_scheduler_handle: Some(work_scheduler_handle),

        negotiator,

        active: true,
    }
}

/// Central server listening for new test run runs and results.
struct QueueServer {
    queues: SharedRuns,
    public_negotiator_addr: SocketAddr,
}

/// An error that happens in the construction or execution of the queue server.
///
/// Does not include errors in the handling of requests to the queue, but does include errors in
/// the acception or dispatch of connections.
#[derive(Debug, Error)]
pub enum QueueServerError {
    /// An IO-related error.
    #[error("{0}")]
    Io(#[from] io::Error),

    /// Any other opaque error that occurred.
    #[error("{0}")]
    Other(#[from] AnyError),
}

#[derive(Clone)]
struct QueueServerCtx {
    queues: SharedRuns,
    public_negotiator_addr: SocketAddr,
    handshake_ctx: Arc<Box<dyn net_async::ServerHandshakeCtx>>,
}

impl QueueServer {
    fn new(queues: SharedRuns, public_negotiator_addr: SocketAddr) -> Self {
        Self {
            queues,
            public_negotiator_addr,
        }
    }

    async fn start(
        self,
        server_listener: Box<dyn net_async::ServerListener>,
        mut shutdown: ShutdownReceiver,
    ) -> Result<(), QueueServerError> {
        let Self {
            queues,
            public_negotiator_addr,
        } = self;

        let ctx = QueueServerCtx {
            queues,
            public_negotiator_addr,
            handshake_ctx: Arc::new(server_listener.handshake_ctx()),
        };

        enum Task {
            HandleConn(UnverifiedServerStream),
        }
        use Task::*;

        loop {
            let task = tokio::select! {
                conn = server_listener.accept() => {
                    match conn {
                        Ok((conn, _)) => HandleConn(conn),
                        Err(e) => {
                            tracing::error!("error accepting connection to queue: {:?}", e);
                            continue;
                        }
                    }
                }
                _ = shutdown.recv_shutdown_immediately() => {
                    break;
                }
            };

            match task {
                HandleConn(client) => {
                    let ctx = ctx.clone();
                    tokio::spawn(async move {
                        let result = Self::handle(ctx, client).await;
                        if let Err(error) = result {
                            log_entityful_error!(error, "error handling connection to queue: {}")
                        }
                    });
                }
            }
        }

        Ok(())
    }

    #[inline(always)]
    async fn handle(
        ctx: QueueServerCtx,
        stream: net_async::UnverifiedServerStream,
    ) -> Result<(), EntityfulError> {
        let mut stream = ctx
            .handshake_ctx
            .handshake(stream)
            .await
            .located(here!())
            .no_entity()?;
        let Request { entity, message } = net_protocol::async_read(&mut stream)
            .await
            .located(here!())
            .no_entity()?;

        let result: OpaqueResult<()> = match message {
            Message::HealthCheck => Self::handle_healthcheck(entity, stream).await,
            Message::ActiveTestRuns => {
                Self::handle_active_test_runs(ctx.queues, entity, stream).await
            }
            Message::NegotiatorInfo {
                run_id,
                deprecations,
            } => {
                log_deprecations(entity, run_id, deprecations);
                Self::handle_negotiator_info(entity, stream, ctx.public_negotiator_addr).await
            }
            Message::CancelRun(run_id) => {
                // Immediately ACK the cancellation and drop the stream
                net_protocol::async_write(
                    &mut stream,
                    &net_protocol::queue::AckTestCancellation {},
                )
                .await
                .located(here!())
                .entity(entity)?;

                // cancellation connection might exit without FIN ACK - allow disconnect here.
                let _shutdown = stream.shutdown().await;
                drop(stream);

                Self::handle_run_cancellation(ctx.queues, entity, run_id).await
            }
            Message::ManifestResult(run_id, manifest_result) => {
                Self::handle_manifest_result(ctx.queues, entity, run_id, manifest_result, stream)
                    .await
            }
            Message::WorkerResult(run_id, results) => {
                // Recording and sending the test result back to the abq test client may
                // be expensive, with multiple IO transactions. There is no reason to block the
                // client connection on that; recall that the worker side of the connection will
                // move on to the next test as soon as it sends a test result back.
                //
                // So, we have no use for the connection as soon as we've parsed the test results out.
                // The worker will have been waiting for us to notify them once the parsing is
                // complete (XREF https://github.com/rwx-research/abq/issues/281), so notify them
                // now and close out our side of the connection.
                net_protocol::async_write(&mut stream, &net_protocol::queue::AckTestResults {})
                    .await
                    .located(here!())
                    .entity(entity)?;
                // Worker connection might exit without FIN ACK - allow disconnect here.
                let _shutdown = stream.shutdown().await;
                drop(stream);

                // Record the test results and notify the test client out-of-band.
                Self::handle_worker_results(ctx.queues, entity, run_id, results).await
            }

            Message::WorkerRanAllTests(run_id) => {
                let notification_time = time::Instant::now();

                net_protocol::async_write(
                    &mut stream,
                    &net_protocol::queue::AckWorkerRanAllTests {},
                )
                .await
                .located(here!())
                .entity(entity)?;
                drop(stream);

                Self::handle_worker_ran_all_tests_notification(
                    ctx.queues,
                    run_id,
                    entity,
                    notification_time,
                )
                .await
            }

            Message::RunStatus(run_id) => {
                Self::handle_total_run_result_request(ctx.queues, run_id, entity, stream).await
            }
        };

        result.map_err(|error| EntityfulError {
            error,
            entity: Some(entity),
        })
    }

    #[inline(always)]
    #[instrument(level = "trace", skip(stream))]
    async fn handle_healthcheck(
        entity: Entity,
        mut stream: Box<dyn net_async::ServerStream>,
    ) -> OpaqueResult<()> {
        // Right now nothing interesting is owned by the queue, so nothing extra to check.
        net_protocol::async_write(&mut stream, &net_protocol::health::healthy())
            .await
            .located(here!())?;
        Ok(())
    }

    #[inline(always)]
    #[instrument(level = "trace", skip(queues, stream))]
    async fn handle_active_test_runs(
        queues: SharedRuns,
        entity: Entity,
        mut stream: Box<dyn net_async::ServerStream>,
    ) -> OpaqueResult<()> {
        let active_runs = queues.estimate_num_active_runs();
        let response = net_protocol::queue::ActiveTestRunsResponse { active_runs };
        net_protocol::async_write(&mut stream, &response)
            .await
            .located(here!())?;
        Ok(())
    }

    #[inline(always)]
    #[instrument(level = "trace", skip(stream, public_negotiator_addr))]
    async fn handle_negotiator_info(
        entity: Entity,
        mut stream: Box<dyn net_async::ServerStream>,
        public_negotiator_addr: SocketAddr,
    ) -> OpaqueResult<()> {
        let negotiator_info = NegotiatorInfo {
            negotiator_address: public_negotiator_addr,
            version: abq_utils::VERSION.to_string(),
        };
        net_protocol::async_write(&mut stream, &negotiator_info)
            .await
            .located(here!())?;
        Ok(())
    }

    /// A worker requests cancellation of a test run.
    #[instrument(level = "trace", skip(queues))]
    async fn handle_run_cancellation(
        queues: SharedRuns,
        entity: Entity,
        run_id: RunId,
    ) -> OpaqueResult<()> {
        tracing::info!(?run_id, ?entity, "worker cancelled a test run");

        {
            // Mark the cancellation in the queue first, so that new queries from workers will be
            // told to terminate.
            queues.mark_cancelled(&run_id, CancelReason::User);
        }

        Ok(())
    }

    #[instrument(level = "trace", skip(queues, manifest_result, stream))]
    async fn handle_manifest_result(
        queues: SharedRuns,
        entity: Entity,
        run_id: RunId,
        manifest_result: ManifestResult,
        stream: Box<dyn net_async::ServerStream>,
    ) -> OpaqueResult<()> {
        match manifest_result {
            ManifestResult::Manifest(reported_manifest) => {
                let ReportedManifest {
                    manifest,
                    native_runner_protocol,
                    native_runner_specification,
                } = reported_manifest;

                // Record the manifest for this run in its appropriate queue.
                let (flat_manifest, metadata) = manifest.flatten();

                let native_runner_info = NativeRunnerInfo {
                    protocol_version: native_runner_protocol,
                    specification: *native_runner_specification,
                };

                if !flat_manifest.is_empty() {
                    Self::handle_manifest_success(
                        queues,
                        entity,
                        run_id,
                        flat_manifest,
                        metadata,
                        native_runner_info,
                        stream,
                    )
                    .await
                } else {
                    Self::handle_manifest_empty_or_failure(
                        queues,
                        entity,
                        run_id,
                        Ok(native_runner_info),
                        stream,
                    )
                    .await
                }
            }
            ManifestResult::TestRunnerError { error, output } => {
                Self::handle_manifest_empty_or_failure(
                    queues,
                    entity,
                    run_id,
                    Err((error, output)),
                    stream,
                )
                .await
            }
        }
    }

    #[instrument(level = "trace", skip(queues, flat_manifest))]
    async fn handle_manifest_success(
        queues: SharedRuns,
        entity: Entity,
        run_id: RunId,
        flat_manifest: Vec<TestSpec>,
        init_metadata: MetadataMap,
        native_runner_info: NativeRunnerInfo,
        mut stream: Box<dyn net_async::ServerStream>,
    ) -> OpaqueResult<()> {
        let manifest_received_time = time::Instant::now();

        tracing::info!(?run_id, ?entity, size=?flat_manifest.len(), "received manifest");

        net_protocol::async_write(&mut stream, &net_protocol::queue::AckManifest {})
            .await
            .located(here!())?;

        let added_manifest = queues.add_manifest(&run_id, flat_manifest.clone(), init_metadata);

        match added_manifest {
            AddedManifest::Added {
                worker_connection_times,
            } => {
                log_workers_waited_for_manifest_latency(
                    &run_id,
                    worker_connection_times,
                    manifest_received_time,
                );
            }
            AddedManifest::RunCancelled => {
                // If the run was already cancelled, there is nothing for us to do.
                tracing::info!(?run_id, ?entity, "received manifest for cancelled run");
            }
        }

        Ok(())
    }

    #[instrument(level = "trace", skip(queues))]
    async fn handle_manifest_empty_or_failure(
        queues: SharedRuns,
        entity: Entity,
        run_id: RunId,
        manifest_result: Result<
            NativeRunnerInfo,         /* empty manifest */
            (String, CapturedOutput), /* error manifest */
        >,
        mut stream: Box<dyn net_async::ServerStream>,
    ) -> OpaqueResult<()> {
        // If a worker failed to generate a manifest, or the manifest is empty,
        // we're going to immediately end the test run.
        //
        // In the former case this indicates a failure in the underlying test runners,
        // and in the latter case we have nothing to do.
        //
        // Either way the steps we have to take are the same, just the status of
        // the test run differs.

        let is_empty_manifest = manifest_result.is_ok();
        if is_empty_manifest {
            tracing::info!(?run_id, ?entity, "exiting early on empty manifest");
        } else {
            tracing::info!(?run_id, ?entity, "failure notification for manifest");
        }

        {
            // Immediately ACK to the worker responsible for the manifest, so they can relinquish
            // blocking and exit.
            net_protocol::async_write(&mut stream, &net_protocol::queue::AckManifest {})
                .await
                .located(here!())?;
            drop(stream);
        }

        // Remove the active run state.
        if is_empty_manifest {
            queues.mark_empty_manifest_complete(run_id);
        } else {
            queues.mark_failed_to_receive_manifest(run_id);
        }

        Ok(())
    }

    #[instrument(level = "trace", skip(_queues))]
    async fn handle_worker_results(
        _queues: SharedRuns,
        entity: Entity,
        run_id: RunId,
        results: Vec<AssociatedTestResults>,
    ) -> OpaqueResult<()> {
        // IFTTT: handle_fired_timeout

        tracing::debug!("got result");

        let _num_results = results.len();

        {
            // TODO do something with the results, eventually!
            let _ = results;
        }

        Ok(())
    }

    #[instrument(level = "trace", skip(queues))]
    async fn handle_worker_ran_all_tests_notification(
        queues: SharedRuns,
        run_id: RunId,
        entity: Entity,
        notification_time: time::Instant,
    ) -> OpaqueResult<()> {
        queues.mark_worker_complete(&run_id, entity, notification_time);

        tracing::debug!(?run_id, ?entity, "worker-ran-all-tests notification");

        Ok(())
    }

    #[instrument(level = "trace", skip(queues, stream))]
    async fn handle_total_run_result_request(
        queues: SharedRuns,
        run_id: RunId,
        entity: Entity,
        mut stream: Box<dyn net_async::ServerStream>,
    ) -> OpaqueResult<()> {
        let response = {
            let queues = queues;
            queues
                .get_run_status(&run_id)
                .ok_or("Invalid state: worker is requesting run result for non-existent ID")
                .located(here!())?
        };

        net_protocol::async_write(&mut stream, &response)
            .await
            .located(here!())?;

        Ok(())
    }
}

fn log_deprecations(entity: Entity, run_id: RunId, deprecations: meta::DeprecationRecord) {
    for deprecation in deprecations.extract() {
        tracing::warn!(?entity, ?run_id, ?deprecation, "deprecation");
    }
}

/// Sends work as workers request it.
/// This does not schedule work in any interesting way today, but it may in the future.
struct WorkScheduler {
    queues: SharedRuns,
    persist_manifest: SharedPersistManifest,
}

/// An error that happens in the construction or execution of the work-scheduling server.
///
/// Does not include errors in the handling of requests to the server, but does include errors in
/// the acception or dispatch of connections.
#[derive(Debug, Error)]
pub enum WorkSchedulerError {
    /// An IO-related error.
    #[error("{0}")]
    Io(#[from] io::Error),

    /// Any other opaque error that occurred.
    #[error("{0}")]
    Other(#[from] AnyError),
}

#[derive(Clone)]
struct SchedulerCtx {
    queues: SharedRuns,
    handshake_ctx: Arc<Box<dyn net_async::ServerHandshakeCtx>>,
    persist_manifest: SharedPersistManifest,
}

impl WorkScheduler {
    async fn start(
        self,
        listener: Box<dyn net_async::ServerListener>,
        mut shutdown: ShutdownReceiver,
    ) -> Result<(), WorkSchedulerError> {
        let Self {
            queues,
            persist_manifest,
        } = self;
        let ctx = SchedulerCtx {
            queues,
            handshake_ctx: Arc::new(listener.handshake_ctx()),
            persist_manifest,
        };

        loop {
            let (client, _) = tokio::select! {
                conn = listener.accept() => {
                    match conn {
                        Ok(conn) => conn,
                        Err(e) => {
                            tracing::error!("error accepting connection to work server: {:?}", e);
                            continue;
                        }
                    }
                }
                _ = shutdown.recv_shutdown_immediately() => {
                    break;
                }
            };

            tokio::spawn({
                let ctx = ctx.clone();
                async move {
                    let result = Self::handle_work_conn(ctx, client).await;
                    if let Err(error) = result {
                        log_entityful_error!(error, "error handling connection to work server: {}")
                    }
                }
            });
        }

        Ok(())
    }

    #[instrument(level = "trace", skip(ctx, stream))]
    async fn handle_work_conn(
        ctx: SchedulerCtx,
        stream: net_async::UnverifiedServerStream,
    ) -> Result<(), EntityfulError> {
        let mut stream = ctx
            .handshake_ctx
            .handshake(stream)
            .await
            .located(here!())
            .no_entity()?;

        // Get the run this worker wants work for.
        let work_server::Request { entity, message } = net_protocol::async_read(&mut stream)
            .await
            .located(here!())
            .no_entity()?;

        use work_server::Message::*;
        match message {
            HealthCheck => {
                let write_result =
                    net_protocol::async_write(&mut stream, &net_protocol::health::healthy()).await;
                if let Err(err) = write_result {
                    tracing::warn!("error sending health check: {}", err.to_string());
                }
            }
            InitContext { run_id } => {
                let init_metadata = ctx.queues.init_metadata(&run_id, entity);

                use net_protocol::work_server::{InitContext, InitContextResponse};
                let response = match init_metadata {
                    InitMetadata::Metadata(init_meta) => {
                        InitContextResponse::InitContext(InitContext { init_meta })
                    }
                    InitMetadata::RunAlreadyCompleted { cancelled } => {
                        InitContextResponse::RunAlreadyCompleted { cancelled }
                    }
                    InitMetadata::WaitingForManifest => InitContextResponse::WaitingForManifest,
                };

                net_protocol::async_write(&mut stream, &response)
                    .await
                    .located(here!())
                    .entity(entity)?;
            }
            PersistentWorkerNextTestsConnection(run_id) => {
                tokio::spawn(Self::start_persistent_next_tests_requests_task(
                    ctx.queues,
                    ctx.persist_manifest,
                    run_id,
                    stream,
                    entity,
                ));
            }
            RetryManifestPartition { run_id, entity } => {
                use net_protocol::work_server::RetryManifestResponse;

                let opt_manifest = ctx.queues.get_retry_manifest(&run_id, entity);

                use RetryManifestState::*;
                let response = match opt_manifest {
                    Manifest(man) => RetryManifestResponse::Manifest(man),
                    FetchFromPersistence => {
                        fetch_persisted_manifest(&ctx.persist_manifest, &run_id, entity).await
                    }
                    RunNotFound => RetryManifestResponse::RunDoesNotExist,
                    ManifestNeverReceived => RetryManifestResponse::ManifestNeverReceived,
                    NotYetPersisted => RetryManifestResponse::NotYetPersisted,
                };

                net_protocol::async_write(&mut stream, &response)
                    .await
                    .located(here!())
                    .entity(entity)?;
            }
        }

        Ok(())
    }

    async fn start_persistent_next_tests_requests_task(
        queues: SharedRuns,
        persist_manifest: SharedPersistManifest,
        run_id: RunId,
        mut conn: Box<dyn net_async::ServerStream>,
        entity: Entity,
    ) -> OpaqueResult<()> {
        use net_protocol::work_server::NextTestRequest;

        loop {
            let NextTestRequest {} = tokio::select! {
                opt_read_error = net_protocol::async_read(&mut conn) => {
                    // If we fail to read or write, simply fall out of the task -
                    // the worker will re-connect as necessary.
                    //
                    // TODO: if we fail to write tests out, we have lost tests to be run in the
                    // manifest. Currently this does not cause runs to hang, as we will end up
                    // timing out the test run when the last test in the queue is handed out.
                    // However, the behavior could still be made better, in particular by
                    // re-enqueuing dropped tests.
                    // See https://github.com/rwx-research/abq/issues/185.
                    opt_read_error.located(here!())?
                }
            };

            use net_protocol::work_server::NextTestResponse;

            // Pull the next bundle of work.
            let next_work_result = queues.next_work(entity, &run_id);

            let NextWorkResult {
                bundle,
                status,
                opt_persistent_manifest_plan,
            } = next_work_result;

            let response = NextTestResponse::Bundle(bundle);

            net_protocol::async_write(&mut conn, &response)
                .await
                .located(here!())?;

            if let Some(persist_manifest_plan) = opt_persistent_manifest_plan {
                // Our last task will be to execute manifest persistence.
                run_manifest_persistence_task(
                    &persist_manifest,
                    &run_id,
                    persist_manifest_plan,
                    entity,
                )
                .await;

                log_assert!(
                    status.reached_end_of_tests(),
                    ?run_id,
                    ?status,
                    "got a manifest plan, but did not reach end of tests"
                );
                return Ok(());
            }

            if status.reached_end_of_tests() {
                // Exit, since the worker should not ask us for tests again.
                return Ok(());
            }
        }
    }
}

async fn run_manifest_persistence_task(
    persist_manifest: &SharedPersistManifest,
    run_id: &RunId,
    persist_manifest_plan: PersistManifestPlan<'_>,
    entity: Entity,
) {
    let task = manifest::make_persistence_task(persist_manifest.borrowed(), persist_manifest_plan);

    let span = tracing::info_span!("running manifest persistence task", ?run_id, ?entity);
    let _enter = span.enter();
    if let Err(error) = task.await.entity(entity) {
        log_entityful_error!(error, "failed to execute manifest persistence job: {}");
    }
}

#[instrument(level = "trace", skip(persist_manifest))]
async fn fetch_persisted_manifest(
    persist_manifest: &SharedPersistManifest,
    run_id: &RunId,
    entity: Entity,
) -> work_server::RetryManifestResponse {
    let manifest_result = persist_manifest
        .get_partition_for_entity(run_id, entity.tag)
        .await;

    match manifest_result.entity(entity) {
        Ok(manifest) => {
            let manifest = manifest
                .into_iter()
                .map(NextWork::Work)
                .chain(std::iter::once(NextWork::EndOfWork))
                .collect();
            RetryManifestResponse::Manifest(NextWorkBundle { work: manifest })
        }
        Err(error) => {
            log_entityful_error!(
                error,
                "manifest marked as persisted, but its loading failed: {}"
            );
            RetryManifestResponse::FailedToLoad
        }
    }
}

#[cfg(test)]
use abq_utils::net_protocol::runners::ProtocolWitness;

#[cfg(test)]
fn fake_test_spec(proto: ProtocolWitness) -> TestSpec {
    use abq_utils::net_protocol::{runners::TestCase, workers::WorkId};

    TestSpec {
        test_case: TestCase::new(proto, "fake-test", Default::default()),
        work_id: WorkId::new(),
    }
}

#[cfg(test)]
mod test {
    use std::{io, time::Instant};

    use crate::{
        persistence::manifest::in_memory::InMemoryPersistor,
        queue::{AddedManifest, CancelReason, QueueServer, RunStatus, SharedRuns, WorkScheduler},
    };
    use abq_run_n_times::n_times;
    use abq_test_utils::{accept_handshake, assert_scoped_log, one_nonzero_usize};
    use abq_utils::{
        auth::{
            build_strategies, Admin, AdminToken, ClientAuthStrategy, ServerAuthStrategy, User,
            UserToken,
        },
        exit::ExitCode,
        net_opt::{ClientOptions, ServerOptions},
        net_protocol::{
            self,
            entity::Entity,
            queue::{
                AckManifest, AssociatedTestResults, NativeRunnerInfo, NegotiatorInfo, TestSpec,
            },
            runners::{NativeRunnerSpecification, TestCase, TestResult},
            work_server,
            workers::{RunId, WorkId},
        },
        server_shutdown::ShutdownManager,
        tls::{ClientTlsStrategy, ServerTlsStrategy},
    };
    use abq_with_protocol_version::with_protocol_version;
    use abq_workers::negotiate::{AssignedRun, AssignedRunStatus};
    use tracing_test::traced_test;

    fn build_random_strategies() -> (
        ServerAuthStrategy,
        ClientAuthStrategy<User>,
        ClientAuthStrategy<Admin>,
    ) {
        build_strategies(UserToken::new_random(), AdminToken::new_random())
    }

    #[tokio::test]
    #[traced_test]
    async fn bad_message_doesnt_take_down_server() {
        let server = QueueServer::new(Default::default(), "0.0.0.0:0".parse().unwrap());

        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let listener = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let (mut shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();
        let server_task = tokio::spawn(server.start(listener, shutdown_rx));

        let client = client_opts.build_async().unwrap();
        let mut conn = client.connect(server_addr).await.unwrap();
        net_protocol::async_write(&mut conn, &"bad message")
            .await
            .unwrap();
        let _fail: io::Result<u64> = net_protocol::async_read(&mut conn).await;

        shutdown_tx.shutdown_immediately().unwrap();
        server_task.await.unwrap().unwrap();

        assert_scoped_log("abq_queue::queue", "error handling connection");
    }

    #[tokio::test]
    async fn queue_server_healthcheck() {
        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let server = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();

        let (mut server_shutdown_tx, server_shutdown_rx) = ShutdownManager::new_pair();

        let queue_server = QueueServer {
            queues: Default::default(),
            public_negotiator_addr: "0.0.0.0:0".parse().unwrap(),
        };
        let queue_handle = tokio::spawn(queue_server.start(server, server_shutdown_rx));

        let client = client_opts.build_async().unwrap();
        let mut conn = client.connect(server_addr).await.unwrap();

        net_protocol::async_write(
            &mut conn,
            &net_protocol::queue::Request {
                entity: Entity::local_client(),
                message: net_protocol::queue::Message::HealthCheck,
            },
        )
        .await
        .unwrap();
        let health_msg: net_protocol::health::Health =
            net_protocol::async_read(&mut conn).await.unwrap();

        assert_eq!(health_msg, net_protocol::health::healthy());

        server_shutdown_tx.shutdown_immediately().unwrap();
        queue_handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn work_server_healthcheck() {
        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let server = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();

        let (mut server_shutdown_tx, server_shutdown_rx) = ShutdownManager::new_pair();

        let work_scheduler = WorkScheduler {
            queues: Default::default(),
            persist_manifest: InMemoryPersistor::shared(),
        };
        let work_server_handle = tokio::spawn(work_scheduler.start(server, server_shutdown_rx));

        let client = client_opts.build_async().unwrap();
        let mut conn = client.connect(server_addr).await.unwrap();
        let request = work_server::Request {
            entity: Entity::local_client(),
            message: work_server::Message::HealthCheck,
        };
        net_protocol::async_write(&mut conn, &request)
            .await
            .unwrap();
        let health_msg: net_protocol::health::Health =
            net_protocol::async_read(&mut conn).await.unwrap();

        assert_eq!(health_msg, net_protocol::health::healthy());

        server_shutdown_tx.shutdown_immediately().unwrap();
        work_server_handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn bad_message_doesnt_take_down_work_scheduling_server() {
        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let server = WorkScheduler {
            queues: Default::default(),
            persist_manifest: InMemoryPersistor::shared(),
        };

        let listener = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (mut shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();
        let server_task = tokio::spawn(server.start(listener, shutdown_rx));

        let client = client_opts.build_async().unwrap();
        let mut conn = client.connect(server_addr).await.unwrap();
        net_protocol::async_write(&mut conn, &"bad message")
            .await
            .unwrap();
        let _fail: io::Result<u64> = net_protocol::async_read(&mut conn).await;

        shutdown_tx.shutdown_immediately().unwrap();
        server_task.await.unwrap().unwrap();

        assert_scoped_log("abq_queue::queue", "error handling connection");
    }

    #[tokio::test]
    #[traced_test]
    async fn connecting_to_queue_server_with_auth_okay() {
        let negotiator_addr = "0.0.0.0:0".parse().unwrap();
        let server = QueueServer::new(Default::default(), negotiator_addr);

        let (server_auth, client_auth, _) =
            build_strategies(UserToken::new_random(), AdminToken::new_random());
        let server_opts = ServerOptions::new(server_auth, ServerTlsStrategy::no_tls());
        let client_opts = ClientOptions::new(client_auth, ClientTlsStrategy::no_tls());

        let listener = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (mut shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();
        let server_task = tokio::spawn(server.start(listener, shutdown_rx));

        let client = client_opts.build_async().unwrap();
        let mut conn = client.connect(server_addr).await.unwrap();
        net_protocol::async_write(
            &mut conn,
            &net_protocol::queue::Request {
                entity: Entity::local_client(),
                message: net_protocol::queue::Message::NegotiatorInfo {
                    run_id: RunId::unique(),
                    deprecations: Default::default(),
                },
            },
        )
        .await
        .unwrap();

        let NegotiatorInfo {
            negotiator_address: recv_negotiator_addr,
            version,
        } = net_protocol::async_read(&mut conn).await.unwrap();
        assert_eq!(negotiator_addr, recv_negotiator_addr);
        assert_eq!(version, abq_utils::VERSION);

        shutdown_tx.shutdown_immediately().unwrap();
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn connecting_to_queue_server_with_no_auth_fails() {
        let negotiator_addr = "0.0.0.0:0".parse().unwrap();
        let server = QueueServer::new(Default::default(), negotiator_addr);

        let (server_auth, _, _) = build_random_strategies();

        let server_opts = ServerOptions::new(server_auth, ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let listener = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (mut shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();
        let server_task = tokio::spawn(server.start(listener, shutdown_rx));

        let client = client_opts.build_async().unwrap();
        let mut conn = client.connect(server_addr).await.unwrap();
        net_protocol::async_write(
            &mut conn,
            &net_protocol::queue::Request {
                entity: Entity::local_client(),
                message: net_protocol::queue::Message::NegotiatorInfo {
                    run_id: RunId::unique(),
                    deprecations: Default::default(),
                },
            },
        )
        .await
        .unwrap();
        let fail: io::Result<NegotiatorInfo> = net_protocol::async_read(&mut conn).await;
        assert!(fail.is_err());

        shutdown_tx.shutdown_immediately().unwrap();
        server_task.await.unwrap().unwrap();

        assert_scoped_log("abq_queue::queue", "error handling connection");
    }

    #[tokio::test]
    #[traced_test]
    async fn connecting_to_work_server_with_auth_okay() {
        let server = WorkScheduler {
            queues: Default::default(),
            persist_manifest: InMemoryPersistor::shared(),
        };

        let (server_auth, client_auth, _) = build_random_strategies();
        let server_opts = ServerOptions::new(server_auth, ServerTlsStrategy::no_tls());
        let client_opts = ClientOptions::new(client_auth, ClientTlsStrategy::no_tls());

        let listener = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (mut shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();
        let server_task = tokio::spawn(server.start(listener, shutdown_rx));

        let client = client_opts.build_async().unwrap();
        let mut conn = client.connect(server_addr).await.unwrap();
        let request = work_server::Request {
            entity: Entity::local_client(),
            message: work_server::Message::HealthCheck,
        };
        net_protocol::async_write(&mut conn, &request)
            .await
            .unwrap();

        let health_msg: net_protocol::health::Health =
            net_protocol::async_read(&mut conn).await.unwrap();

        assert_eq!(health_msg, net_protocol::health::healthy());

        shutdown_tx.shutdown_immediately().unwrap();
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn connecting_to_work_server_with_no_auth_fails() {
        let server = WorkScheduler {
            queues: Default::default(),
            persist_manifest: InMemoryPersistor::shared(),
        };

        let (server_auth, _, _) = build_random_strategies();
        let server_opts = ServerOptions::new(server_auth, ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let listener = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (mut shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();
        let server_task = tokio::spawn(server.start(listener, shutdown_rx));

        let client = client_opts.build_async().unwrap();
        let mut conn = client.connect(server_addr).await.unwrap();
        let request = work_server::Request {
            entity: Entity::local_client(),
            message: work_server::Message::HealthCheck,
        };
        net_protocol::async_write(&mut conn, &request)
            .await
            .unwrap();

        shutdown_tx.shutdown_immediately().unwrap();
        server_task.await.unwrap().unwrap();

        assert_scoped_log("abq_queue::queue", "error handling connection");
    }

    #[test]
    fn no_active_runs_when_non_enqueued() {
        let queues = SharedRuns::default();

        assert_eq!(queues.estimate_num_active_runs(), 0);
    }

    #[test]
    #[with_protocol_version]
    fn active_runs_when_some_waiting_on_manifest() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let _ = queues.find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1));

        assert_eq!(queues.estimate_num_active_runs(), 1);
    }

    #[test]
    #[with_protocol_version]
    fn active_runs_when_running() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let _ = queues.find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1));
        let added = queues.add_manifest(&run_id, vec![], Default::default());
        assert!(matches!(added, AddedManifest::Added { .. }));

        assert_eq!(queues.estimate_num_active_runs(), 1);
    }

    #[test]
    #[with_protocol_version]
    fn active_runs_when_all_done() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let _ = queues.find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1));
        let added = queues.add_manifest(&run_id, vec![], Default::default());
        assert!(matches!(added, AddedManifest::Added { .. }));
        queues.try_mark_reached_end_of_manifest(&run_id, ExitCode::SUCCESS);

        assert_eq!(queues.estimate_num_active_runs(), 0);
    }

    #[test]
    #[with_protocol_version]
    fn active_runs_multiple_in_various_states() {
        let queues = SharedRuns::default();

        let run_id1 = RunId::unique(); // DONE
        let run_id2 = RunId::unique(); // DONE
        let run_id3 = RunId::unique(); // WAITING
        let run_id4 = RunId::unique(); // RUNNING
        let expected_active = 2;

        for run_id in [&run_id1, &run_id2, &run_id3, &run_id4] {
            let _ = queues.find_or_create_run(run_id, one_nonzero_usize(), Entity::runner(0, 1));
        }

        for run_id in [&run_id1, &run_id2, &run_id4] {
            let added = queues.add_manifest(run_id, vec![], Default::default());
            assert!(matches!(added, AddedManifest::Added { .. }));
        }

        for run_id in [run_id1, run_id2] {
            queues.try_mark_reached_end_of_manifest(&run_id, ExitCode::SUCCESS);
        }

        assert_eq!(queues.estimate_num_active_runs(), expected_active);
    }

    #[test]
    #[with_protocol_version]
    fn mark_cancellation_when_some_waiting_on_manifest() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let _ = queues.find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1));

        assert_eq!(queues.estimate_num_active_runs(), 1);

        queues.mark_cancelled(&run_id, CancelReason::User);

        assert_eq!(queues.estimate_num_active_runs(), 0);
        assert!(matches!(
            queues.get_run_status(&run_id).unwrap(),
            RunStatus::Cancelled
        ));
    }

    #[test]
    #[with_protocol_version]
    fn mark_cancellation_when_running() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let _ = queues.find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1));

        let added = queues.add_manifest(&run_id, vec![], Default::default());
        assert!(matches!(added, AddedManifest::Added { .. }));

        assert_eq!(queues.estimate_num_active_runs(), 1);

        queues.mark_cancelled(&run_id, CancelReason::User);

        assert_eq!(queues.estimate_num_active_runs(), 0);
        assert!(matches!(
            queues.get_run_status(&run_id).unwrap(),
            RunStatus::Cancelled
        ));
    }

    #[test]
    #[with_protocol_version]
    fn track_worker_entities_over_run_state() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let worker0 = Entity::runner(1, 1);
        let worker1 = Entity::runner(2, 2);
        let worker2 = Entity::runner(3, 3);

        // worker0 creates run
        {
            let assigned_lookup = queues.find_or_create_run(&run_id, one_nonzero_usize(), worker0);
            assert_eq!(
                assigned_lookup,
                AssignedRunStatus::Run(AssignedRun::Fresh {
                    should_generate_manifest: true
                })
            );
            assert_eq!(queues.get_run_status(&run_id), Some(RunStatus::Active));
            assert_eq!(queues.get_active_workers(&run_id), vec![worker0]);
        }

        // worker1 attaches
        {
            let assigned_lookup = queues.find_or_create_run(&run_id, one_nonzero_usize(), worker1);
            assert_eq!(
                assigned_lookup,
                AssignedRunStatus::Run(AssignedRun::Fresh {
                    should_generate_manifest: false
                })
            );
            assert_eq!(queues.get_run_status(&run_id), Some(RunStatus::Active));
            assert_eq!(queues.get_active_workers(&run_id), vec![worker0, worker1]);
        }

        // manifest is populated
        {
            let added_manifest = queues.add_manifest(&run_id, vec![], Default::default());
            assert!(matches!(added_manifest, AddedManifest::Added { .. }));
            assert_eq!(queues.get_run_status(&run_id), Some(RunStatus::Active));
            assert_eq!(queues.get_active_workers(&run_id), vec![worker0, worker1]);
        }

        // worker2 attaches
        {
            let assigned_lookup = queues.find_or_create_run(&run_id, one_nonzero_usize(), worker2);
            assert_eq!(
                assigned_lookup,
                AssignedRunStatus::Run(AssignedRun::Fresh {
                    should_generate_manifest: false
                })
            );
            assert_eq!(queues.get_run_status(&run_id), Some(RunStatus::Active));
            assert_eq!(
                queues.get_active_workers(&run_id),
                vec![worker0, worker1, worker2]
            );
        }

        // worker2 done
        {
            queues.mark_worker_complete(&run_id, worker2, Instant::now());
            assert_eq!(queues.get_run_status(&run_id), Some(RunStatus::Active));
            assert_eq!(queues.get_active_workers(&run_id), vec![worker0, worker1]);
        }

        // whole run done
        {
            queues.try_mark_reached_end_of_manifest(&run_id, ExitCode::SUCCESS);
            assert_eq!(
                queues.get_run_status(&run_id),
                Some(RunStatus::InitialManifestDone {
                    num_active_workers: 2
                })
            );
            assert_eq!(queues.get_active_workers(&run_id), vec![worker0, worker1]);
        }

        // worker1 done
        {
            queues.mark_worker_complete(&run_id, worker1, Instant::now());
            assert_eq!(
                queues.get_run_status(&run_id),
                Some(RunStatus::InitialManifestDone {
                    num_active_workers: 1
                })
            );
            assert_eq!(queues.get_active_workers(&run_id), vec![worker0]);
        }

        // worker0 done
        {
            queues.mark_worker_complete(&run_id, worker0, Instant::now());
            assert_eq!(
                queues.get_run_status(&run_id),
                Some(RunStatus::InitialManifestDone {
                    num_active_workers: 0
                })
            );
            assert_eq!(queues.get_active_workers(&run_id), vec![]);
        }
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn cancel_before_manifest_is_received_then_receive_manifest() {
        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let fake_server = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let fake_server_addr = fake_server.local_addr().unwrap();

        let client = client_opts.build_async().unwrap();

        // Build up our initial state - run is registered, no manifest yet
        let run_id = RunId::unique();
        let queues = {
            let queues = SharedRuns::default();
            let _ = queues.find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1));
            queues
        };

        let entity = Entity::runner(0, 1);

        // Cancel the run, make sure we exit smoothly
        {
            let cancellation_fut =
                QueueServer::handle_run_cancellation(queues.clone(), entity, run_id.clone());

            let cancellation_end = cancellation_fut.await;
            assert!(cancellation_end.is_ok());
        }

        // Toss the manifest over to the queue. It should be accepted but not cause any deviation
        // since the test run is already cancelled.
        {
            let (client_res, server_res) = futures::join!(
                client.connect(fake_server_addr),
                accept_handshake(&*fake_server)
            );
            let (mut client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());

            let handle_manifest_fut = QueueServer::handle_manifest_success(
                queues.clone(),
                entity,
                run_id.clone(),
                vec![TestSpec {
                    test_case: TestCase::new(proto, "test1", Default::default()),
                    work_id: WorkId::new(),
                }],
                Default::default(),
                NativeRunnerInfo {
                    protocol_version: proto.get_version(),
                    specification: NativeRunnerSpecification::fake(),
                },
                server_conn,
            );
            let recv_manifest_ack_fut =
                net_protocol::async_read::<_, AckManifest>(&mut client_conn);

            let (handle_manifest_end, recv_ack_end) =
                futures::join!(handle_manifest_fut, recv_manifest_ack_fut);
            assert!(handle_manifest_end.is_ok());
            assert!(recv_ack_end.is_ok());
        }

        // Check the run state is cancelled
        assert!(matches!(
            queues.get_run_status(&run_id).unwrap(),
            RunStatus::Cancelled {}
        ));
    }

    #[tokio::test]
    #[n_times(1_000)]
    #[with_protocol_version]
    async fn cancel_and_receive_manifest_concurrently() {
        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let fake_server = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let fake_server_addr = fake_server.local_addr().unwrap();

        let client = client_opts.build_async().unwrap();

        // Build up our initial state - run is registered, no manifest yet
        let run_id = RunId::unique();
        let queues = {
            let queues = SharedRuns::default();
            let _ = queues.find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1));
            queues
        };

        let entity = Entity::runner(0, 1);

        // Cancel the run, make sure we exit smoothly
        let do_cancellation_fut = async {
            let cancellation_fut =
                QueueServer::handle_run_cancellation(queues.clone(), entity, run_id.clone());

            let cancellation_end = cancellation_fut.await;
            assert!(cancellation_end.is_ok());
        };

        // Toss the manifest over to the queue. It should be accepted but not cause any deviation
        // since the test run is already cancelled.
        let handle_manifest_fut = {
            let (client_res, server_res) = futures::join!(
                client.connect(fake_server_addr),
                accept_handshake(&*fake_server)
            );
            let (mut client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());

            let queues = queues.clone();
            let run_id = run_id.clone();

            async move {
                let handle_manifest_fut = QueueServer::handle_manifest_success(
                    queues,
                    entity,
                    run_id,
                    vec![TestSpec {
                        test_case: TestCase::new(proto, "test1", Default::default()),
                        work_id: WorkId::new(),
                    }],
                    Default::default(),
                    NativeRunnerInfo {
                        protocol_version: proto.get_version(),
                        specification: NativeRunnerSpecification::fake(),
                    },
                    server_conn,
                );
                let recv_manifest_ack_fut =
                    net_protocol::async_read::<_, AckManifest>(&mut client_conn);

                let (handle_manifest_end, recv_ack_end) =
                    futures::future::join(handle_manifest_fut, recv_manifest_ack_fut).await;
                assert!(handle_manifest_end.is_ok());
                assert!(recv_ack_end.is_ok());
            }
        };

        // Run both the cancellation and manifest-handling concurrently; it should succeed and end
        // up in a cancelled state.
        let ((), ()) = futures::join!(do_cancellation_fut, handle_manifest_fut);

        // Check the run state is cancelled
        assert!(matches!(
            queues.get_run_status(&run_id).unwrap(),
            RunStatus::Cancelled {}
        ));
    }

    #[tokio::test]
    #[n_times(1_000)]
    #[with_protocol_version]
    async fn receiving_cancellation_during_last_test_results_is_cancellation() {
        let run_id = RunId::unique();
        let queues = {
            let queues = SharedRuns::default();
            let _ = queues.find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1));
            let added = queues.add_manifest(&run_id, vec![], Default::default());
            assert!(matches!(added, AddedManifest::Added { .. }));
            queues
        };

        let entity = Entity::runner(0, 1);

        let result = AssociatedTestResults::fake(WorkId::new(), vec![TestResult::fake()]);
        let send_last_result_fut = QueueServer::handle_worker_results(
            queues.clone(),
            entity,
            run_id.clone(),
            vec![result],
        );
        let cancellation_fut =
            QueueServer::handle_run_cancellation(queues.clone(), entity, run_id.clone());

        let (send_last_result_end, cancellation_end) =
            futures::join!(send_last_result_fut, cancellation_fut);

        assert!(send_last_result_end.is_ok());
        assert!(cancellation_end.is_ok());

        assert!(matches!(
            queues.get_run_status(&run_id).unwrap(),
            RunStatus::Cancelled {}
        ));
    }
}

#[cfg(test)]
mod test_pull_work {
    use abq_test_utils::one_nonzero_usize;
    use abq_utils::net_protocol::{
        entity::Entity,
        workers::{NextWork, RunId, WorkerTest},
    };
    use abq_with_protocol_version::with_protocol_version;

    use crate::queue::{
        fake_test_spec, JobQueue, NextWorkResult, PulledTestsStatus, RunData, RunState, RunStatus,
        SharedRuns,
    };

    #[test]
    #[with_protocol_version]
    fn pull_last_tests_for_final_test_run_attempt_shows_tests_complete() {
        // Set up the queue so only one test is pulled, and there are a total of two attempts.
        let queue = JobQueue::new(vec![
            WorkerTest {
                spec: fake_test_spec(proto),
                run_number: 1,
            },
            WorkerTest {
                spec: fake_test_spec(proto),
                run_number: 2,
            },
        ]);

        let batch_size_hint = one_nonzero_usize();

        let has_work = RunState::HasWork {
            queue,
            init_metadata: Default::default(),
            batch_size_hint,
            active_workers: Default::default(),
        };

        let run_id = RunId::unique();
        let queues = SharedRuns::default();
        queues.set_state(run_id.clone(), has_work, Some(RunData { batch_size_hint }));

        // First pull. There should be additional tests following it.
        {
            let next_work = queues.next_work(Entity::runner(0, 1), &run_id);

            assert!(matches!(next_work, NextWorkResult { bundle, status, .. }
                if bundle.work.len() == 1 && matches!(status, PulledTestsStatus::MoreTestsRemaining)
            ));

            assert!(matches!(
                queues.get_run_status(&run_id),
                Some(RunStatus::Active)
            ));
        }

        // Second pull. Now we reach the end of all tests and all test run attempts.
        {
            let next_work = queues.next_work(Entity::runner(0, 1), &run_id);

            assert!(matches!(next_work, NextWorkResult { bundle, status, .. }
                if bundle.work.len() == 2 // 1 - test, 1 - end of work
                && matches!(bundle.work.last(), Some(NextWork::EndOfWork))
                && matches!(status, PulledTestsStatus::PulledLastTest { .. })
            ));

            assert!(matches!(
                queues.get_run_status(&run_id),
                Some(RunStatus::InitialManifestDone { .. })
            ));
        }

        // Now, if another worker pulls work it should get nothing other than an end-of-test marker,
        // and be told we're all done.
        {
            let next_work = queues.next_work(Entity::runner(0, 1), &run_id);

            assert!(matches!(next_work, NextWorkResult { bundle, status, .. }
                if bundle.work.len() == 1
                && matches!(bundle.work.last(), Some(NextWork::EndOfWork))
                && matches!(status, PulledTestsStatus::QueueWasEmpty)
            ));

            assert!(matches!(
                queues.get_run_status(&run_id),
                Some(RunStatus::InitialManifestDone { .. })
            ));
        }
    }
}

#[cfg(test)]
mod retry_manifest {
    use std::{net::SocketAddr, time::Duration};

    use crate::{
        job_queue::JobQueue,
        persistence::manifest::{in_memory::InMemoryPersistor, SharedPersistManifest},
        queue::{fake_test_spec, NextWorkResult, RunData, RunState, SharedRuns, WorkScheduler},
    };
    use abq_test_utils::{one_nonzero_usize, spec, worker_test};
    use abq_utils::{
        auth::{ClientAuthStrategy, ServerAuthStrategy},
        net_async,
        net_opt::{ClientOptions, ServerOptions},
        net_protocol::{
            self,
            entity::Entity,
            work_server,
            workers::{NextWork, RunId, WorkerTest, INIT_RUN_NUMBER},
        },
        server_shutdown::ShutdownManager,
        tls::{ClientTlsStrategy, ServerTlsStrategy},
    };
    use abq_with_protocol_version::with_protocol_version;
    use abq_workers::negotiate::{AssignedRun, AssignedRunStatus};
    use ntest::timeout;
    use tokio::task::JoinHandle;

    #[test]
    #[with_protocol_version]
    fn worker_told_to_pull_retry_manifest() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let worker0 = Entity::runner(1, 1);
        let worker0_shadow = Entity::runner(1, 1);
        assert_ne!(worker0.id, worker0_shadow.id);
        assert_eq!(worker0.tag, worker0_shadow.tag);

        let test1 = fake_test_spec(proto);
        let test2 = fake_test_spec(proto);
        let test3 = fake_test_spec(proto);

        // Create run, add manifest by worker0
        {
            let _ = queues.find_or_create_run(&run_id, one_nonzero_usize(), worker0);
            let _ = queues.add_manifest(
                &run_id,
                vec![test1.clone(), test2, test3],
                Default::default(),
            );
        }

        // worker0 pulls tests
        {
            let NextWorkResult { bundle, .. } = queues.next_work(worker0, &run_id);
            assert_eq!(
                bundle.work,
                vec![NextWork::Work(WorkerTest::new(
                    test1.clone(),
                    INIT_RUN_NUMBER
                ))]
            );
        }

        // Suppose worker0 sporadically dies. Now worker0_shadow should be told to pull a retry
        // manifest.
        let assigned = queues.find_or_create_run(&run_id, one_nonzero_usize(), worker0_shadow);
        assert_eq!(assigned, AssignedRunStatus::Run(AssignedRun::Retry));
    }

    struct WorkSchedulerBuilder(WorkScheduler);
    impl WorkSchedulerBuilder {
        fn new(queues: SharedRuns) -> Self {
            Self(WorkScheduler {
                queues,
                persist_manifest: InMemoryPersistor::shared(),
            })
        }
        fn with_persist_manifest(mut self, persist: SharedPersistManifest) -> Self {
            self.0.persist_manifest = persist;
            self
        }
    }

    struct WorkSchedulerState {
        shutdown_tx: ShutdownManager,
        server_task: JoinHandle<()>,
        server_addr: SocketAddr,
        conn: Box<dyn net_async::ClientStream>,
        client: Box<dyn net_async::ConfiguredClient>,
    }

    async fn build_work_scheduler(builder: WorkSchedulerBuilder) -> WorkSchedulerState {
        let server = builder.0;

        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let listener = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();
        let server_task =
            tokio::spawn(async move { server.start(listener, shutdown_rx).await.unwrap() });

        let client = client_opts.build_async().unwrap();
        let conn = client.connect(server_addr).await.unwrap();

        WorkSchedulerState {
            shutdown_tx,
            server_task,
            server_addr,
            conn,
            client,
        }
    }

    #[tokio::test]
    #[with_protocol_version]
    #[timeout(1000)]
    async fn pulling_end_of_manifest_eventually_persists_manifest() {
        let spec1 = spec(1);
        let spec2 = spec(2);

        let manifest = vec![
            WorkerTest::new(spec1.clone(), 1),
            WorkerTest::new(spec2.clone(), 1),
        ];

        let run_id = RunId::unique();

        let queues = {
            let queue = JobQueue::new(manifest);

            let batch_size_hint = 2.try_into().unwrap();

            let has_work = RunState::HasWork {
                queue,
                init_metadata: Default::default(),
                batch_size_hint,
                active_workers: Default::default(),
            };

            let queues = SharedRuns::default();
            queues.set_state(run_id.clone(), has_work, Some(RunData { batch_size_hint }));
            queues
        };

        let persist_manifest = InMemoryPersistor::shared();

        let WorkSchedulerState {
            mut shutdown_tx,
            server_task,
            server_addr,
            mut conn,
            client,
        } = build_work_scheduler(
            WorkSchedulerBuilder::new(queues.clone())
                .with_persist_manifest(persist_manifest.clone()),
        )
        .await;

        let entity = Entity::runner(1, 1);

        // Open a persistent work connection, ask for a bundle of tests.
        let request = work_server::Request {
            entity,
            message: work_server::Message::PersistentWorkerNextTestsConnection(run_id.clone()),
        };
        net_protocol::async_write(&mut conn, &request)
            .await
            .unwrap();
        net_protocol::async_write(&mut conn, &work_server::NextTestRequest {})
            .await
            .unwrap();

        let work_server::NextTestResponse::Bundle(bundle) =
            net_protocol::async_read(&mut conn).await.unwrap();

        assert_eq!(
            bundle.work,
            vec![
                worker_test(spec1.clone(), 1),
                worker_test(spec2.clone(), 1),
                NextWork::EndOfWork
            ]
        );

        // In time, we should see that the manifest was persisted.
        loop {
            let mut conn = client.connect(server_addr).await.unwrap();
            let request = work_server::Request {
                entity: Entity::runner(1, 1),
                message: work_server::Message::RetryManifestPartition {
                    run_id: run_id.clone(),
                    entity: Entity::runner(1, 1),
                },
            };
            net_protocol::async_write(&mut conn, &request)
                .await
                .unwrap();
            let response = net_protocol::async_read(&mut conn).await.unwrap();

            use work_server::RetryManifestResponse::*;
            match response {
                Manifest(man) => {
                    assert_eq!(
                        man.work,
                        vec![
                            worker_test(spec1, 1),
                            worker_test(spec2, 1),
                            NextWork::EndOfWork
                        ]
                    );
                    break;
                }
                NotYetPersisted => {
                    tokio::time::sleep(Duration::from_micros(100)).await;
                    continue;
                }
                response => unreachable!("{response:?}"),
            }
        }

        shutdown_tx.shutdown_immediately().unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn pull_retry_manifest_for_active_work() {
        let queues = SharedRuns::default();
        let run_id = RunId::unique();

        let test1 = fake_test_spec(proto);
        let test2 = fake_test_spec(proto);
        let test3 = fake_test_spec(proto);

        let entity = Entity::runner(0, 1);

        let _ = queues.find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1));
        let _ = queues.add_manifest(
            &run_id,
            vec![test1.clone(), test2.clone(), test3],
            Default::default(),
        );

        // Prime the queue, pulling two entries for the entity
        {
            let NextWorkResult { bundle, .. } = queues.next_work(entity, &run_id);
            assert_eq!(
                bundle.work,
                vec![NextWork::Work(WorkerTest::new(
                    test1.clone(),
                    INIT_RUN_NUMBER
                ))]
            );
        }
        {
            let NextWorkResult { bundle, .. } = queues.next_work(entity, &run_id);
            assert_eq!(
                bundle.work,
                vec![NextWork::Work(WorkerTest::new(
                    test2.clone(),
                    INIT_RUN_NUMBER
                ))]
            );
        }

        let WorkSchedulerState {
            mut shutdown_tx,
            server_task,
            mut conn,
            ..
        } = build_work_scheduler(WorkSchedulerBuilder::new(queues)).await;

        let request = work_server::Request {
            entity,
            message: work_server::Message::RetryManifestPartition { run_id, entity },
        };
        net_protocol::async_write(&mut conn, &request)
            .await
            .unwrap();
        let response: work_server::RetryManifestResponse =
            net_protocol::async_read(&mut conn).await.unwrap();

        match response {
            work_server::RetryManifestResponse::Manifest(manifest) => {
                assert_eq!(
                    manifest.work,
                    vec![
                        NextWork::Work(WorkerTest::new(test1, INIT_RUN_NUMBER)),
                        NextWork::Work(WorkerTest::new(test2, INIT_RUN_NUMBER)),
                        NextWork::EndOfWork
                    ]
                )
            }
            _ => panic!(),
        }

        shutdown_tx.shutdown_immediately().unwrap();
        server_task.await.unwrap();
    }
}
