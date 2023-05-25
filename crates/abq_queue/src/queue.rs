use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::ops::{ControlFlow, Deref};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;
use std::{io, time};

use abq_utils::auth::ServerAuthStrategy;
use abq_utils::exit::ExitCode;
use abq_utils::net_async::{self, UnverifiedServerStream};
use abq_utils::net_opt::ServerOptions;
use abq_utils::net_protocol::entity::{Entity, Tag};
use abq_utils::net_protocol::error::RetryManifestError;
use abq_utils::net_protocol::queue::{
    AssociatedTestResults, CancelReason, NativeRunnerInfo, NegotiatorInfo, Request,
    TestResultsResponse, TestSpec,
};
use abq_utils::net_protocol::results::{self, OpaqueLazyAssociatedTestResults};
use abq_utils::net_protocol::runners::{MetadataMap, StdioOutput};
use abq_utils::net_protocol::work_server::{self, RetryManifestResponse};
use abq_utils::net_protocol::workers::{
    Eow, ManifestResult, NextWorkBundle, ReportedManifest, WorkerTest, INIT_RUN_NUMBER,
};
use abq_utils::net_protocol::{
    self,
    queue::{InvokeWork, Message, RunStatus},
    workers::RunId,
};
use abq_utils::net_protocol::{meta, publicize_addr};
use abq_utils::server_shutdown::{ShutdownManager, ShutdownReceiver};
use abq_utils::tls::ServerTlsStrategy;
use abq_utils::vec_map::VecMap;
use abq_utils::{atomic, illegal_state, log_assert};
use abq_workers::negotiate::{QueueNegotiator, QueueNegotiatorHandle, QueueNegotiatorServerError};
use abq_workers::{AssignedRun, AssignedRunStatus, GetAssignedRun};

use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;
use tracing::instrument;

use crate::job_queue::JobQueue;
use crate::persistence::manifest::{
    self, ManifestPersistedCell, PersistManifestPlan, SharedPersistManifest,
};
use crate::persistence::remote::{LoadedRunState, RemotePersister};
use crate::persistence::results::{
    EligibleForRemoteDump, ResultsPersistedCell, SharedPersistResults,
};
use crate::persistence::run_state::PersistRunStatePlan;
use crate::timeout::{FiredTimeout, RunTimeoutManager, RunTimeoutStrategy, TimeoutReason};
use crate::worker_timings::{log_workers_waited_for_manifest_latency, WorkerTimings};
use crate::worker_tracking::WorkerSet;
use crate::{persistence, prelude::*};

#[derive(Debug)]
enum RunState {
    /// First worker has connected. Waiting for manifest.
    WaitingForManifest {
        /// For the purposes of analytics, records timings of when workers connect prior to the
        /// manifest being generated.
        worker_connection_times: Mutex<WorkerSet<Instant>>,

        batch_size_hint: NonZeroUsize,
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

        /// A tracker of what results have been persisted.
        results_persistence: ResultsPersistedCell,
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

        /// A tracker of what results have been persisted.
        results_persistence: ResultsPersistence,
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

#[derive(Debug)]
enum ResultsPersistence {
    Persisted(ResultsPersistedCell),
    ManifestNeverReceived,
}

const MAX_BATCH_SIZE: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(100) };

/// A individual test run ever invoked on the queue.
struct Run {
    /// The state of the test run.
    state: RunState,
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

struct PersistAfterManifestDone<'a> {
    persist_manifest_plan: PersistManifestPlan<'a>,
    persist_run_state_plan: PersistRunStatePlan<'a>,
}

/// Result of trying to pull a batch of tests for a worker.
struct NextWorkResult<'a> {
    bundle: NextWorkBundle,
    status: PulledTestsStatus,
    opt_persistence: Option<PersistAfterManifestDone<'a>>,
}

#[derive(Debug)]
#[must_use]
enum AddedManifest {
    Added {
        /// For the purposes of analytics, timings of when workers first connected
        /// prior to the manifest being generated.
        worker_connection_times: WorkerTimings,
        /// The size of the manifest.
        manifest_size_nonce: u64,
        /// The cell the [manifest summary][net_protocol::results::Summary] should be written into.
        results_persistence: ResultsPersistedCell,
    },
    RunCancelled,
}

#[must_use]
enum RecordedEmptyManifest {
    Recorded {
        /// The cell the [manifest summary][net_protocol::results::Summary] should be written into.
        results_persistence: ResultsPersistedCell,
    },
    RunCancelled,
}

#[derive(Debug)]
enum RetryManifestState {
    Manifest(NextWorkBundle),
    FetchFromPersistence,
    NotYetPersisted,
    Error(RetryManifestError),
}

#[derive(Debug, Error)]
enum WriteResultsError {
    #[error("attempting to write results for unregistered run")]
    RunNotFound,
    #[error("attempting to write results before manifest received")]
    WaitingForManifest,
    #[error("attempting to write results when manifest failed to be generated")]
    ManifestNeverReceived,
    #[error("attempting to write results for cancelled run")]
    RunCancelled,
}

#[derive(Debug, Error, PartialEq, Eq)]
enum ReadResultsError {
    #[error("results cannot be read for an unregistered run")]
    RunNotFound,
    #[error("results cannot be read before manifest is received")]
    WaitingForManifest,
    #[error("a manifest failed to be generated")]
    ManifestNeverReceived,
    #[error("the run was cancelled before all test results were received")]
    RunCancelled,
}

#[derive(Debug)]
enum ReadResultsState {
    /// Results are ready to be retrieved, no active workers are currently seen.
    ReadFromCell(ResultsPersistedCell),
    /// The given workers are still active.
    OutstandingRunners(Vec<Tag>),
}

#[derive(Debug, PartialEq, Eq)]
enum ManifestProgressCancelReason {
    /// The run was cancelled because no progress was made in receiving the manifest.
    ManifestNotReceived,
    /// The run was cancelled because no progress was made in popping tests off the manifest.
    NoTestProgress,
}

#[derive(Debug, PartialEq, Eq)]
enum ManifestProgressResult {
    /// The run was cancelled.
    Cancelled(ManifestProgressCancelReason),
    /// The run was not cancelled because the run is already complete.
    RunComplete,
    /// Progress was made in the manifest.
    NewProgress { newly_observed_test_index: usize },
}

impl AllRuns {
    /// Finds a queue for a run, or creates a new one if the run is observed as fresh.
    /// If the given run ID already has an associated queue, an error is returned.
    pub async fn find_or_create_run(
        &self,
        run_id: &RunId,
        batch_size_hint: NonZeroUsize,
        entity: Entity,
        remote: &RemotePersister,
    ) -> AssignedRunStatus {
        if !self.runs.read().contains_key(run_id) {
            match self
                .try_create_run(run_id, batch_size_hint, entity, remote)
                .await
            {
                ControlFlow::Break(assigned) => return assigned,
                ControlFlow::Continue(()) => {
                    // Pass through, the run was not actually fresh and is now
                    // emplaced in the run state.
                }
            }
        }

        let runs = self.runs.read();
        let run = match runs.get(run_id) {
            Some(st) => st.read(),
            None => {
                illegal_state!(
                    "a run must always be populated when looking up for worker",
                    ?run_id
                );
                return AssignedRunStatus::RunUnknown;
            }
        };

        match &run.state {
            RunState::WaitingForManifest {
                worker_connection_times,
                batch_size_hint: _,
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
                            "worker reconnecting for out-of-process retry manifest during active run"
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
                if seen_workers.read().contains_by_tag(&entity) {
                    // This worker was already involved in this run; assume it's connecting for an
                    // out-of-process retry.
                    tracing::info!(
                        ?run_id,
                        ?entity,
                        "worker reconnecting for out-of-process retry manifest after initial run"
                    );
                    AssignedRunStatus::Run(AssignedRun::Retry)
                } else {
                    let exit_code = *new_worker_exit_code;
                    tracing::info!(
                        ?run_id,
                        ?entity,
                        "assigning fresh worker already-completed status for run"
                    );
                    // Fresh workers shouldn't get any new work.
                    // The worker should exit successfully locally, regardless of what the overall
                    // code was.
                    AssignedRunStatus::AlreadyDone { exit_code }
                }
            }
            RunState::Cancelled { .. } => AssignedRunStatus::AlreadyDone {
                exit_code: ExitCode::CANCELLED,
            },
        }
    }

    /// Optimistically creates a fresh state for a run. If at the time of creation, the run is
    /// still not present in the queue state, the fresh state will be emplaced in the queue state.
    ///
    /// Otherwise, returning [ControlFlow::Continue] should fall back to loading the run from the
    /// queue state.
    #[inline]
    async fn try_create_run(
        &self,
        run_id: &RunId,
        batch_size_hint: NonZeroUsize,
        entity: Entity,
        remote: &RemotePersister,
    ) -> ControlFlow<AssignedRunStatus> {
        let result = async move {
            match remote.try_load_run_state(run_id).await? {
                LoadedRunState::Found(run_state) => {
                    self.try_load_remote_run(run_id, entity, run_state)
                }
                LoadedRunState::NotFound => {
                    // This run is not present in the remote persister, so we can create a fresh run.
                    Ok(self.try_create_globally_fresh_run(run_id, batch_size_hint, entity))
                }
                LoadedRunState::IncompatibleSchemaVersion { found, expected } => {
                    tracing::info!(
                        ?run_id,
                        ?found,
                        ?expected,
                        "found an existing run, but its state has an incompatible schema version"
                    );
                    Ok(self.try_create_globally_fresh_run(run_id, batch_size_hint, entity))
                }
            }
        }
        .await;

        match result {
            Ok(control_flow) => control_flow,
            Err(error) => {
                let err_string = error.error.to_string();
                log_entityful_error!(
                    EntityfulError {
                        error,
                        entity: Some(entity)
                    },
                    "fatal error trying to create fresh run: {}"
                );
                ControlFlow::Break(AssignedRunStatus::FatalError(err_string))
            }
        }
    }

    fn try_create_globally_fresh_run(
        &self,
        run_id: &RunId,
        batch_size_hint: NonZeroUsize,
        entity: Entity,
    ) -> ControlFlow<AssignedRunStatus> {
        let run = {
            let mut worker_timings = WorkerSet::default();
            worker_timings.insert_by_tag(entity, time::Instant::now());

            // The run ID is fresh; create a new queue for it.
            Run {
                state: RunState::WaitingForManifest {
                    worker_connection_times: Mutex::new(worker_timings),
                    batch_size_hint,
                },
            }
        };

        // Possible TOCTOU race here, so we must check whether the run is still missing
        // before we add it freshly.
        //
        // If the run state is now populated, defer to that, since it can only
        // monotonically advance the state machine (run states move forward and are
        // never removed).
        let mut runs = self.runs.write();
        if !runs.contains_key(run_id) {
            // NB: Always add first for conversative estimation.
            self.num_active.fetch_add(1, atomic::ORDERING);

            let old_run = runs.insert(run_id.clone(), RwLock::new(run));
            log_assert!(old_run.is_none(), "can only be called when run is fresh!");

            tracing::info!(?run_id, ?entity, "created fresh run");

            ControlFlow::Break(AssignedRunStatus::Run(AssignedRun::Fresh {
                should_generate_manifest: true,
            }))
        } else {
            ControlFlow::Continue(())
        }
    }

    fn try_load_remote_run(
        &self,
        run_id: &RunId,
        entity: Entity,
        run_state: persistence::run_state::RunState,
    ) -> OpaqueResult<ControlFlow<AssignedRunStatus>> {
        let persistence::run_state::RunState {
            new_worker_exit_code,
            init_metadata,
            seen_workers,
        } = run_state;

        // No worker is currently active, as this is the first time we're seeing them on this
        // queue instance.
        let worker_active = false;
        let seen_workers = seen_workers
            .into_iter()
            .map(|worker| (worker, worker_active))
            .collect();

        let run = Run {
            state: RunState::InitialManifestDone {
                new_worker_exit_code,
                seen_workers: RwLock::new(seen_workers),
                init_metadata,
                manifest_persistence: ManifestPersistence::Persisted(
                    ManifestPersistedCell::new_already_persisted(),
                ),
                results_persistence: ResultsPersistence::Persisted(ResultsPersistedCell::new(
                    run_id.clone(),
                )),
            },
        };

        // Possible TOCTOU race here, so we must check whether the run is still missing
        // before we add it freshly.
        //
        // If the run state is now populated, defer to that.
        let mut runs = self.runs.write();
        if !runs.contains_key(run_id) {
            let old_run = runs.insert(run_id.clone(), RwLock::new(run));
            log_assert!(old_run.is_none(), "can only be called when run is fresh!");

            tracing::info!(
                ?run_id,
                ?entity,
                "loaded previously-executed run from remote"
            );
        }

        // In either case, tell the caller to continue reading from the run state, since we've now
        // loaded in a run that only deals with out-of-process retries.
        Ok(ControlFlow::Continue(()))
    }

    /// Adds the initial manifest for an ABQ test suite run.
    fn add_manifest(
        &self,
        run_id: &RunId,
        flat_manifest: Vec<TestSpec>,
        init_metadata: MetadataMap,
    ) -> AddedManifest {
        let runs = self.runs.read();

        let mut run = runs.get(run_id).expect("no run recorded").write();

        let (worker_connection_times, batch_size_hint) = match &mut run.state {
            RunState::WaitingForManifest {
                worker_connection_times,
                batch_size_hint,
            } => {
                // expected state, pass through
                let timings = Mutex::into_inner(std::mem::take(worker_connection_times));
                (timings, *batch_size_hint)
            }
            RunState::Cancelled { .. } => {
                // If cancelled, do nothing.
                return AddedManifest::RunCancelled;
            }
            RunState::HasWork { .. } | RunState::InitialManifestDone { .. } => {
                illegal_state!(
                    "can only provide manifest while waiting for manifest",
                    ?run_id
                );
                return AddedManifest::RunCancelled;
            }
        };

        let manifest_size_nonce = flat_manifest.len() as u64;
        let work_from_manifest = flat_manifest.into_iter().map(|spec| WorkerTest {
            spec,
            run_number: INIT_RUN_NUMBER,
        });

        let queue = JobQueue::new(work_from_manifest.collect());

        let mut active_workers = WorkerSet::with_capacity(worker_connection_times.len());
        let mut worker_conn_times = VecMap::with_capacity(worker_connection_times.len());
        for (worker, time) in worker_connection_times {
            active_workers.insert_by_tag(worker, None);
            worker_conn_times.insert(worker, time);
        }

        let results_persistence = ResultsPersistedCell::new(run_id.clone());

        run.state = RunState::HasWork {
            queue,
            batch_size_hint,
            init_metadata,
            active_workers: Mutex::new(active_workers),
            results_persistence: results_persistence.clone(),
        };

        AddedManifest::Added {
            worker_connection_times: worker_conn_times,
            manifest_size_nonce,
            results_persistence,
        }
    }

    fn init_metadata(&self, run_id: &RunId, entity: Entity) -> InitMetadata {
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
                    tracing::debug!(
                        ?run_id,
                        ?entity,
                        "assigning init metadata to out-of-process retry"
                    );
                    seen_workers.write().insert_by_tag(entity, true);
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

    fn next_work<'a>(&self, entity: Entity, run_id: &'a RunId) -> NextWorkResult<'a> {
        let runs = self.runs.read();

        let run = runs.get(run_id).expect("no run recorded").read();

        match &run.state {
            RunState::HasWork {
                queue,
                batch_size_hint,
                active_workers,
                ..
            } => {
                active_workers.lock().insert_by_tag_if_missing(entity, None);
                let (bundle, status) = Self::get_next_work_online(queue, entity, *batch_size_hint);
                match status {
                    PulledTestsStatus::MoreTestsRemaining => NextWorkResult {
                        bundle,
                        status,
                        opt_persistence: None,
                    },
                    PulledTestsStatus::PulledLastTest { .. } | PulledTestsStatus::QueueWasEmpty => {
                        drop(run);
                        drop(runs);
                        tracing::debug!(?entity, ?run_id, "saw end of manifest for entity");

                        let opt_persistence =
                            self.try_mark_reached_end_of_manifest(run_id, ExitCode::SUCCESS);

                        NextWorkResult {
                            bundle,
                            status,
                            opt_persistence,
                        }
                    }
                }
            }
            RunState::InitialManifestDone { seen_workers, .. } => {
                // Let the worker know that we've reached the end of the queue.
                seen_workers.write().insert_by_tag_if_missing(entity, true);
                NextWorkResult {
                    bundle: NextWorkBundle::new([], Eow(true)),
                    status: PulledTestsStatus::QueueWasEmpty,
                    opt_persistence: None,
                }
            }
            RunState::Cancelled { .. } => NextWorkResult {
                bundle: NextWorkBundle::new([], Eow(true)),
                status: PulledTestsStatus::QueueWasEmpty,
                opt_persistence: None,
            },
            RunState::WaitingForManifest { .. } => {
                illegal_state!("work can only be requested after initialization metadata, at which point the manifest is known.", ?run_id, ?entity);
                NextWorkResult {
                    bundle: NextWorkBundle::new([], Eow(true)),
                    status: PulledTestsStatus::QueueWasEmpty,
                    opt_persistence: None,
                }
            }
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
        // - I guess to do this we need to split up `NextWorkBundle` for the queue (with borrows) and with the worker (with ownership)?
        let bundle: Vec<WorkerTest> = queue.get_work(entity.tag, batch_size).cloned().collect();

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
                Some(_) => {
                    pulled_tests_status = PulledTestsStatus::PulledLastTest;
                }
                None => {
                    pulled_tests_status = PulledTestsStatus::QueueWasEmpty;
                }
            }
        } else {
            // Queue is not empty.
            pulled_tests_status = PulledTestsStatus::MoreTestsRemaining;
        };

        // Let the worker know this is the end, so they don't ask again.
        let eow = Eow(pulled_tests_status.reached_end_of_tests());

        let bundle = NextWorkBundle { work: bundle, eow };

        (bundle, pulled_tests_status)
    }

    fn get_write_results_cell(
        &self,
        run_id: &RunId,
    ) -> Result<(ResultsPersistedCell, EligibleForRemoteDump), WriteResultsError> {
        use WriteResultsError::*;

        let runs = self.runs.read();
        let run = match runs.get(run_id) {
            Some(run) => run.read(),
            None => return Err(RunNotFound),
        };

        match &run.state {
            RunState::WaitingForManifest { .. } => Err(WaitingForManifest),
            RunState::HasWork {
                results_persistence,
                ..
            } => {
                // Since there are still tests in the manifest, we're not yet eligible for a dump
                // to the remote. We will be once the manifest is emptied, at which point we'll be
                // receiving the test results that live at the tail of the manifest.
                let elibible_for_remote_dump = EligibleForRemoteDump::No;

                Ok((results_persistence.clone(), elibible_for_remote_dump))
            }
            RunState::InitialManifestDone {
                results_persistence,
                ..
            } => match results_persistence {
                ResultsPersistence::Persisted(cell) => {
                    Ok((cell.clone(), EligibleForRemoteDump::Yes))
                }
                ResultsPersistence::ManifestNeverReceived => Err(ManifestNeverReceived),
            },
            RunState::Cancelled { .. } => Err(RunCancelled),
        }
    }

    fn get_read_results_cell(&self, run_id: &RunId) -> Result<ReadResultsState, ReadResultsError> {
        use ReadResultsError::*;

        let runs = self.runs.read();
        let run = match runs.get(run_id) {
            Some(run) => run.read(),
            None => return Err(RunNotFound),
        };

        match &run.state {
            RunState::WaitingForManifest { .. } => Err(WaitingForManifest),
            RunState::HasWork { active_workers, .. } => {
                // While we still have work in the initial manifest, we have outstanding tests to
                // persist; don't permit reading right now.
                let active_workers = active_workers.lock();

                let active_runners = active_workers
                    .iter()
                    .filter(|(_, done_time)| done_time.is_none())
                    .map(|(e, _)| e.tag)
                    .collect();
                Ok(ReadResultsState::OutstandingRunners(active_runners))
            }
            RunState::InitialManifestDone {
                results_persistence,
                seen_workers,
                ..
            } => {
                match results_persistence {
                    ResultsPersistence::Persisted(cell) => {
                        // Query the state of the active workers.
                        // If we don't have any pending, the results can be fetched (subject to the
                        // linearizability consistency model; see [crate::persistence::results]).
                        let workers = seen_workers.read();
                        let mut active_workers = workers
                            .iter()
                            .filter(|(_, is_active)| *is_active)
                            .map(|(e, _)| e.tag)
                            .peekable();

                        if active_workers.peek().is_none() {
                            Ok(ReadResultsState::ReadFromCell(cell.clone()))
                        } else {
                            Ok(ReadResultsState::OutstandingRunners(
                                active_workers.collect(),
                            ))
                        }
                    }
                    ResultsPersistence::ManifestNeverReceived => Err(ManifestNeverReceived),
                }
            }
            RunState::Cancelled { .. } => Err(RunCancelled),
        }
    }

    fn get_retry_manifest(&self, run_id: &RunId, entity: Entity) -> RetryManifestState {
        let runs = self.runs.read();
        let run = match runs.get(run_id) {
            Some(run) => run.read(),
            None => return RetryManifestState::Error(RetryManifestError::RunDoesNotExist),
        };

        match &run.state {
            RunState::WaitingForManifest { .. } => {
                illegal_state!(
                    "attempting to fetch retry manifest while waiting for manifest",
                    ?run_id,
                    ?entity
                );
                RetryManifestState::Error(RetryManifestError::ManifestNeverReceived)
            }
            RunState::HasWork {
                queue,
                active_workers,
                ..
            } => {
                // TODO: can we get rid of the clone allocation here?
                //
                // TODO: should we launch discovery of a partition on a blocking async task?
                // If this ever takes a non-trivial amount of time, consider doing so.
                let manifest: Vec<_> = queue
                    .get_partition_for_entity(entity.tag)
                    .cloned()
                    .collect();
                let eow = Eow(true);

                // Mark the worker as active again, regardless of its existing state.
                active_workers.lock().insert_by_tag(entity, None);

                RetryManifestState::Manifest(NextWorkBundle {
                    work: manifest,
                    eow,
                })
            }
            RunState::InitialManifestDone {
                manifest_persistence,
                seen_workers,
                ..
            } => match manifest_persistence {
                ManifestPersistence::Persisted(cell) => {
                    if cell.is_persisted() {
                        // Mark the worker as active again, regardless of its existing state.
                        seen_workers.write().insert_by_tag(entity, true);
                        RetryManifestState::FetchFromPersistence
                    } else {
                        RetryManifestState::NotYetPersisted
                    }
                }
                ManifestPersistence::ManifestNeverReceived => {
                    RetryManifestState::Error(RetryManifestError::ManifestNeverReceived)
                }
                ManifestPersistence::EmptyManifest => {
                    // Ship the empty manifest over.
                    RetryManifestState::Manifest(NextWorkBundle {
                        work: vec![],
                        eow: Eow(true),
                    })
                }
            },
            RunState::Cancelled { .. } => {
                RetryManifestState::Error(RetryManifestError::RunCancelled)
            }
        }
    }

    fn mark_worker_complete(&self, run_id: &RunId, entity: Entity, notification_time: Instant) {
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
    ) -> Option<PersistAfterManifestDone<'a>> {
        let runs = self.runs.read();

        let mut run = runs.get(run_id).expect("no run recorded").write();

        let active_worker_timings;
        let queue;
        let init_metadata;
        let results_persistence;
        match &mut run.state {
            RunState::HasWork {
                queue: this_queue,
                active_workers: this_active_workers,
                init_metadata: this_init_metadata,
                results_persistence: this_results_persistence,
                ..
            } => {
                log_assert!(
                    this_queue.is_at_end(),
                    "Invalid state - queue is not complete!"
                );
                active_worker_timings = Mutex::into_inner(std::mem::take(this_active_workers));
                queue = std::mem::take(this_queue);
                init_metadata = std::mem::take(this_init_metadata);
                results_persistence = this_results_persistence.clone();
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
                illegal_state!(
                    "tried to mark end of manifest while waiting for manifest",
                    ?run_id
                );
                return None;
            }
        };

        let mut seen_workers = WorkerSet::default();

        for (worker, opt_completed) in active_worker_timings {
            // The worker is still active if we don't have a completion time for it.
            let is_active = opt_completed.is_none();
            seen_workers.insert_by_tag(worker, is_active);
        }

        tracing::info!(?run_id, worker_count=?seen_workers.worker_count(), "marking end of manifest");

        // Build the plan to persist the manifest.
        let view = queue.into_manifest_view();
        let (manifest_persisted, persist_manifest_plan) =
            manifest::build_persistence_plan(run_id, view);

        // Build the plan to persist the run state at manifest completion.
        let persist_run_state_plan = PersistRunStatePlan::new(
            run_id,
            persistence::run_state::RunState {
                new_worker_exit_code,
                init_metadata: init_metadata.clone(),
                seen_workers: seen_workers.iter().map(|(worker, _)| *worker).collect(),
            },
        );

        run.state = RunState::InitialManifestDone {
            new_worker_exit_code,
            init_metadata,
            seen_workers: RwLock::new(seen_workers),
            manifest_persistence: ManifestPersistence::Persisted(manifest_persisted),
            results_persistence: ResultsPersistence::Persisted(results_persistence),
        };

        // NB: Always sub last for conversative estimation.
        self.num_active.fetch_sub(1, atomic::ORDERING);

        Some(PersistAfterManifestDone {
            persist_manifest_plan,
            persist_run_state_plan,
        })
    }

    fn mark_failed_to_receive_manifest(&self, run_id: RunId) {
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
                illegal_state!(
                    "attempting to mark failed to receive manifest after manifest was received",
                    ?run_id
                );
                return;
            }
        }

        run.state = RunState::InitialManifestDone {
            new_worker_exit_code: ExitCode::FAILURE,
            init_metadata: Default::default(),
            seen_workers: Default::default(),
            manifest_persistence: ManifestPersistence::ManifestNeverReceived,
            results_persistence: ResultsPersistence::ManifestNeverReceived,
        };

        // NB: Always sub last for conversative estimation.
        self.num_active.fetch_sub(1, atomic::ORDERING);
    }

    /// Marks a run as complete because it had the trivial manifest.
    fn mark_empty_manifest_complete(&self, run_id: RunId) -> RecordedEmptyManifest {
        let runs = self.runs.read();

        let mut run = runs.get(&run_id).expect("no run recorded").write();

        match run.state {
            RunState::WaitingForManifest { .. } => {
                // okay
            }
            RunState::Cancelled { .. } => {
                // No-op, since the run was already cancelled.
                return RecordedEmptyManifest::RunCancelled;
            }
            RunState::HasWork { .. } | RunState::InitialManifestDone { .. } => {
                illegal_state!(
                    "can only mark complete due to manifest while waiting for manifest",
                    ?run_id
                );
                return RecordedEmptyManifest::RunCancelled;
            }
        }

        let results_persistence = ResultsPersistedCell::new(run_id);

        // Trivially successful
        run.state = RunState::InitialManifestDone {
            new_worker_exit_code: ExitCode::SUCCESS,
            init_metadata: Default::default(),
            seen_workers: Default::default(),
            manifest_persistence: ManifestPersistence::EmptyManifest,
            results_persistence: ResultsPersistence::Persisted(results_persistence.clone()),
        };

        // NB: Always sub last for conversative estimation.
        self.num_active.fetch_sub(1, atomic::ORDERING);

        RecordedEmptyManifest::Recorded {
            results_persistence,
        }
    }

    fn mark_cancelled(&self, run_id: &RunId, entity: Entity, reason: CancelReason) {
        let runs = self.runs.read();

        let mut run = runs.get(run_id).expect("no run recorded").write();

        match &run.state {
            RunState::WaitingForManifest { .. }
            | RunState::HasWork { .. }
            | RunState::Cancelled { .. } => {
                // legal cancellation states
            }
            RunState::InitialManifestDone { seen_workers, .. } => {
                // Since we already have issued the full manifest out, don't mark this run as
                // cancelled; this might be a stragling worker or a worker that cancelled an
                // out-of-process retry.
                tracing::info!(
                    ?run_id,
                    "refusing to cancel run whose manifest has already been exhausted"
                );
                // Mark the worker as now-inactive.
                let old_tag = seen_workers.write().insert_by_tag(entity, false);
                log_assert!(
                    old_tag.is_some(),
                    ?entity,
                    ?run_id,
                    "entity was not seen before it marked cancellation"
                );
                return;
            }
        }

        Self::mark_cancelled_help(&mut run.state, &self.num_active, reason)
    }

    fn mark_cancelled_help(run_state: &mut RunState, num_active: &AtomicU64, reason: CancelReason) {
        *run_state = RunState::Cancelled { reason };

        // NB: Always sub last for conversative estimation.
        num_active.fetch_sub(1, atomic::ORDERING);
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

    /// Cancels a test run if no progress in the manifest since `last_observed_test_index` has been
    /// made; otherwise, does nothing.
    ///
    /// Returns `None` if the run does not exist.
    fn handle_manifest_progress_timeout(
        &self,
        run_id: &RunId,
        last_observed_test_index: usize,
    ) -> Option<ManifestProgressResult> {
        let runs = self.runs.read();

        let run = runs.get(run_id)?;
        {
            let run = run.read();
            match Self::query_manifest_progress_status(&run.state, run_id, last_observed_test_index)
            {
                Ok(ok_result) => return Some(ok_result),
                Err(_cancel_reason) => {
                    // This is eligible for cancellation, fall through.
                }
            }
        }

        // This run is eligible for cancellation. We must now check its state again to avoid TOCTOU
        // races; if it is indeed still in a state eligible for cancellation, perform the
        // cancellation.
        let mut run = run.write();
        match Self::query_manifest_progress_status(&run.state, run_id, last_observed_test_index) {
            Ok(ok_result) => Some(ok_result),
            Err(cancel_reason) => {
                Self::mark_cancelled_help(
                    &mut run.state,
                    &self.num_active,
                    CancelReason::ManifestHadNoProgress,
                );
                Some(ManifestProgressResult::Cancelled(cancel_reason))
            }
        }
    }

    /// Checks the status of a manifest's progress. If progress has been made, an appropriate
    /// [ManifestProgressResult] is returned; otherwise, an `Err` value indicates why the run
    /// should be cancelled.
    fn query_manifest_progress_status(
        run_state: &RunState,
        run_id: &RunId,
        last_observed_test_index: usize,
    ) -> Result<ManifestProgressResult, ManifestProgressCancelReason> {
        match run_state {
            RunState::WaitingForManifest { .. } => {
                Err(ManifestProgressCancelReason::ManifestNotReceived)
            }
            RunState::HasWork { queue, .. } => {
                let newly_observed_test_index = queue.read_index();

                log_assert!(
                    newly_observed_test_index >= last_observed_test_index,
                    ?run_id,
                    ?last_observed_test_index,
                    ?newly_observed_test_index,
                    "test index went backwards"
                );

                if newly_observed_test_index > last_observed_test_index {
                    Ok(ManifestProgressResult::NewProgress {
                        newly_observed_test_index,
                    })
                } else {
                    Err(ManifestProgressCancelReason::NoTestProgress)
                }
            }
            RunState::InitialManifestDone { .. } | RunState::Cancelled { .. } => {
                Ok(ManifestProgressResult::RunComplete)
            }
        }
    }

    #[cfg(test)]
    fn get_active_workers(&self, run_id: &RunId) -> Vec<Entity> {
        let runs = self.runs.read();

        let run = runs.get(run_id).expect("no such run").read();

        match &run.state {
            RunState::WaitingForManifest {
                worker_connection_times,
                ..
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
    fn set_state(&self, run_id: RunId, state: RunState) -> Option<RwLock<Run>> {
        let mut runs = self.runs.write();
        runs.insert(run_id, RwLock::new(Run { state }))
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
    /// How results should be persisted.
    pub persist_results: SharedPersistResults,
    /// Answers questions about remote storage of runs.
    pub remote: RemotePersister,
    /// How timeouts should be configured.
    pub run_timeout_strategy: RunTimeoutStrategy,
}

impl QueueConfig {
    /// Creates a [`QueueConfig`] that always binds and advertises on INADDR_ANY, with arbitrary
    /// ports for its servers.
    pub fn new(
        persist_manifest: SharedPersistManifest,
        persist_results: SharedPersistResults,
        remote: RemotePersister,
    ) -> Self {
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
            persist_results,
            remote,
            run_timeout_strategy: RunTimeoutStrategy::RUN_BASED,
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
        persist_results,
        remote,
        run_timeout_strategy,
    } = config;

    let mut shutdown_manager = ShutdownManager::default();

    let timeout_manager = RunTimeoutManager::new(run_timeout_strategy);

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
        let queue_server = QueueServer::new(
            queues,
            public_negotiator_addr,
            persist_results,
            timeout_manager.clone(),
        );
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
            remote: remote.clone(),
        };
        scheduler.start(new_work_server, work_scheduler_shutdown_rx)
    });

    // Provide the execution context a set of workers should attach with, if it is known at the
    // time of polling. We must not to block here, since that can take a lock over the shared
    // queues indefinitely.
    let choose_run_for_worker = ChooseRunForWorker {
        queues: queues.clone(),
        timeout_manager,
        remote,
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

struct ChooseRunForWorker {
    queues: SharedRuns,
    timeout_manager: RunTimeoutManager,
    remote: RemotePersister,
}

#[async_trait]
impl GetAssignedRun for ChooseRunForWorker {
    async fn get_assigned_run(
        &self,
        entity: Entity,
        invoke_work: &InvokeWork,
    ) -> AssignedRunStatus {
        let InvokeWork {
            run_id,
            batch_size_hint,
        } = invoke_work;

        let batch_size_hint =
            if batch_size_hint.get() > MAX_BATCH_SIZE.get() as u64 {
                MAX_BATCH_SIZE
            } else {
                NonZeroUsize::new(batch_size_hint.get().try_into().expect(
                    "u64 batch size must fit into a usize batch size less than MAX_BATCH_SIZE",
                ))
                .unwrap()
            };

        let assigned_run = self
            .queues
            .find_or_create_run(run_id, batch_size_hint, entity, &self.remote)
            .await;

        // Now that we've found a run for this ID, if the run is fresh, enqueue a job to check
        // whether any progress has been made for it later.
        if assigned_run.freshly_created() {
            let timeout_spec = self
                .timeout_manager
                .strategy()
                .wait_for_manifest_progress_duration(0);

            self.timeout_manager
                .insert(run_id.clone(), timeout_spec)
                .await;
        }

        assigned_run
    }
}

/// Central server listening for new test run runs and results.
struct QueueServer {
    queues: SharedRuns,
    persist_results: SharedPersistResults,
    timeout_manager: RunTimeoutManager,
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
    persist_results: SharedPersistResults,
    handshake_ctx: Arc<Box<dyn net_async::ServerHandshakeCtx>>,
    timeout_manager: RunTimeoutManager,
}

impl QueueServer {
    fn new(
        queues: SharedRuns,
        public_negotiator_addr: SocketAddr,
        persist_results: SharedPersistResults,
        timeout_manager: RunTimeoutManager,
    ) -> Self {
        Self {
            queues,
            persist_results,
            timeout_manager,
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
            persist_results,
            timeout_manager,
            public_negotiator_addr,
        } = self;

        let ctx = QueueServerCtx {
            queues,
            public_negotiator_addr,
            persist_results,
            handshake_ctx: Arc::new(server_listener.handshake_ctx()),
            timeout_manager: timeout_manager.clone(),
        };

        enum Task {
            HandleConn(UnverifiedServerStream),
            HandleTimeout(FiredTimeout),
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
                fired_timeout = timeout_manager.next_timeout() => {
                    HandleTimeout(fired_timeout)
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
                HandleTimeout(fired_timeout) => {
                    let queues = ctx.queues.clone();
                    let timeout_manager = ctx.timeout_manager.clone();
                    tokio::spawn(async move {
                        let result = Self::handle_timeout(queues, timeout_manager, fired_timeout)
                            .await
                            .no_entity();
                        if let Err(error) = result {
                            log_entityful_error!(error, "error handling timeout: {}")
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
                Self::handle_manifest_result(
                    ctx.queues,
                    ctx.persist_results,
                    entity,
                    run_id,
                    manifest_result,
                    stream,
                )
                .await
            }
            Message::WorkerResult(run_id, results) => {
                Self::handle_worker_results(
                    ctx.queues,
                    ctx.persist_results,
                    run_id,
                    entity,
                    results,
                    stream,
                )
                .await
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

            Message::TestResults(run_id) => {
                Self::handle_test_results_request(
                    ctx.queues,
                    ctx.persist_results,
                    run_id,
                    entity,
                    stream,
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
            queues.mark_cancelled(&run_id, entity, CancelReason::User);
        }

        Ok(())
    }

    #[instrument(level = "trace", skip_all, fields(run_id=?run_id, entity=?entity))]
    async fn handle_manifest_result(
        queues: SharedRuns,
        persist_results: SharedPersistResults,
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

                if flat_manifest.is_empty() {
                    Self::handle_manifest_empty_or_failure(
                        queues,
                        persist_results,
                        entity,
                        run_id,
                        Ok(native_runner_info),
                        stream,
                    )
                    .await
                } else {
                    Self::handle_manifest_success(
                        queues,
                        persist_results,
                        entity,
                        run_id,
                        flat_manifest,
                        metadata,
                        native_runner_info,
                        stream,
                    )
                    .await
                }
            }
            ManifestResult::TestRunnerError { error, output } => {
                Self::handle_manifest_empty_or_failure(
                    queues,
                    persist_results,
                    entity,
                    run_id,
                    Err((error, output)),
                    stream,
                )
                .await
            }
        }
    }

    #[instrument(level = "trace", skip_all, fields(run_id=?run_id, entity=?entity, size=flat_manifest.len(), native_runner_info=?native_runner_info))]
    async fn handle_manifest_success(
        queues: SharedRuns,
        persist_results: SharedPersistResults,
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
                manifest_size_nonce,
                results_persistence,
            } => {
                log_workers_waited_for_manifest_latency(
                    &run_id,
                    worker_connection_times,
                    manifest_received_time,
                );

                // Write down the summary of the test suite into the persistence cell.
                let summary = net_protocol::results::Summary {
                    manifest_size_nonce,
                    native_runner_info,
                };

                // The manifest was only just added; don't dump to the remote, as that will happen
                // after all results come in.
                let eligible_for_remote_dump = EligibleForRemoteDump::No;
                run_summary_persistence_task(
                    entity,
                    &persist_results,
                    results_persistence,
                    eligible_for_remote_dump,
                    summary,
                )
                .await?;
            }
            AddedManifest::RunCancelled => {
                // If the run was already cancelled, there is nothing for us to do.
                tracing::info!(?run_id, ?entity, "received manifest for cancelled run");
            }
        }

        Ok(())
    }

    #[instrument(level = "trace", skip_all, fields(entity=?entity, run_id=?run_id, is_empty=manifest_result.is_ok()))]
    async fn handle_manifest_empty_or_failure(
        queues: SharedRuns,
        persist_results: SharedPersistResults,
        entity: Entity,
        run_id: RunId,
        manifest_result: Result<
            NativeRunnerInfo,      /* empty manifest */
            (String, StdioOutput), /* error manifest */
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

        if manifest_result.is_ok() {
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
        match manifest_result {
            Ok(native_runner_info) => match queues.mark_empty_manifest_complete(run_id) {
                RecordedEmptyManifest::Recorded {
                    results_persistence,
                } => {
                    let summary = results::Summary {
                        native_runner_info,
                        manifest_size_nonce: 0,
                    };

                    // Since this is the empty manifest and there will be no continuation of the
                    // run, we can go ahead and offload the summary to the remote now.
                    let eligible_for_remote_dump = EligibleForRemoteDump::Yes;
                    run_summary_persistence_task(
                        entity,
                        &persist_results,
                        results_persistence,
                        eligible_for_remote_dump,
                        summary,
                    )
                    .await?
                }
                RecordedEmptyManifest::RunCancelled => {
                    // nothing we can do
                }
            },
            Err(..) => {
                queues.mark_failed_to_receive_manifest(run_id);
            }
        }

        Ok(())
    }

    #[instrument(level = "trace", skip(queues, persist_results))]
    async fn handle_worker_results(
        queues: SharedRuns,
        persist_results: SharedPersistResults,
        run_id: RunId,
        entity: Entity,
        results: Vec<AssociatedTestResults>,
        mut stream: Box<dyn net_async::ServerStream>,
    ) -> OpaqueResult<()> {
        tracing::debug!(?entity, ?run_id, "got result");

        // Build a plan for persistence of the result, then immediately chuck the test result ACK
        // over the wire before executing the plan.
        //
        // We must build the plan beforehand to properly account the number of persistence tasks
        // in progress; otherwise, we face a race where
        //   - a worker sends test results
        //   - we enqueue their processing, not accounting the pending task beforehand
        //   - ACK the worker and the worker exits
        //   - a test-fetching request is sent is is processes before the results persistence task
        //     begins execution, so the fact that there is a pending persistence is not seen.
        //
        // To avoid this, do a (relatively) cheap fetch and update of the number of persistence
        // tasks, then send the ACK, since we don't want to block the workers on the actual
        // persistence.
        let cell_result = queues.get_write_results_cell(&run_id).located(here!());

        let plan_result = match cell_result.as_ref() {
            Ok((cell, eligible_for_remote_dump)) => Ok(cell.build_persist_results_plan(
                &persist_results,
                results,
                *eligible_for_remote_dump,
            )),
            Err(e) => Err(e),
        };

        let opt_ack_error =
            net_protocol::async_write(&mut stream, &net_protocol::queue::AckTestResults {})
                .await
                .located(here!());
        // Worker connection might exit without FIN ACK - allow disconnect here.
        let _shutdown = stream.shutdown().await;
        drop(stream);

        match plan_result {
            Ok(plan) => {
                let result = plan.execute().await;
                result.or(opt_ack_error)
            }
            Err(_) => Err(cell_result.unwrap_err()),
        }
    }

    #[instrument(level = "trace", skip(queues, persist_results))]
    async fn handle_test_results_request(
        queues: SharedRuns,
        persist_results: SharedPersistResults,
        run_id: RunId,
        entity: Entity,
        mut stream: Box<dyn net_async::ServerStream>,
    ) -> OpaqueResult<()> {
        let response;
        let result;

        enum Response {
            One(TestResultsResponse),
            Chunk(OpaqueLazyAssociatedTestResults),
        }
        use Response::*;

        match queues.get_read_results_cell(&run_id).located(here!()) {
            Ok(state) => match state {
                ReadResultsState::ReadFromCell(cell) => {
                    // Happy path: actually attempt the retrieval. Let's see what comes up.
                    match cell.retrieve(&persist_results).await {
                        Some(Ok(results)) => {
                            response = Chunk(results);
                            result = Ok(());
                        }
                        None => {
                            response = One(TestResultsResponse::Pending);
                            result = Ok(());
                        }
                        Some(Err(e)) => {
                            response = One(TestResultsResponse::Error(e.to_string()));
                            result = Err(e.error.to_string().located(here!()));
                        }
                    };
                }
                ReadResultsState::OutstandingRunners(tags) => {
                    response = One(TestResultsResponse::OutstandingRunners(tags));
                    result = Ok(());
                }
            },
            Err(e) => {
                response = One(TestResultsResponse::Error(e.error.to_string()));
                result = Err(e);
            }
        };

        match response {
            One(response) => {
                net_protocol::async_write(&mut stream, &response)
                    .await
                    .located(here!())?;
            }
            Chunk(results) => {
                // Split the results into chunks that will fit in individual messages over the
                // network.
                //
                // Chunking is CPU-bound and typically quite fast if there are no chunks,
                // but might eat allocations if there is indeed material chunking to do.
                // Since this is usually run by a client after the critical section of a test run,
                // move it to a dedicated CPU region to avoid starving the main queue responder
                // threads.
                let chunks = tokio::task::spawn_blocking(|| {
                    TestResultsResponse::chunk_results(results).located(here!())
                })
                .await
                .located(here!())??;

                let mut iter = chunks.into_iter().peekable();
                while let Some(chunk) = iter.next() {
                    let response = TestResultsResponse::Results {
                        chunk,
                        final_chunk: iter.peek().is_none(),
                    };
                    net_protocol::async_write(&mut stream, &response)
                        .await
                        .located(here!())?;
                }
            }
        }

        result
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

    /// Handles fired timeouts for test results, instuted by a [RunTimeoutManager].
    /// There is one class of timeout, always fired for a queue:
    ///
    /// - [TimeoutReason::WaitForManifestProgress] - fired on an interval to check whether progress
    ///   has been made in popping the manifest for a run.
    ///   If no progress has been made, the run is considered to be stuck and is cancelled.
    ///   If progress has been made but the manifest is still active, the timeout is re-enqueued.
    async fn handle_timeout(
        queues: SharedRuns,
        timeout_manager: RunTimeoutManager,
        timeout: FiredTimeout,
    ) -> OpaqueResult<()> {
        let FiredTimeout {
            run_id,
            reason,
            after,
        } = timeout;

        let TimeoutReason::WaitForManifestProgress {
            last_observed_test_index,
        } = reason;

        let opt_result = queues.handle_manifest_progress_timeout(&run_id, last_observed_test_index);

        match opt_result {
            Some(result) => match result {
                ManifestProgressResult::Cancelled(reason) => {
                    tracing::warn!(
                        ?run_id,
                        ?reason,
                        ?after,
                        "run cancelled because manifest made no progress"
                    );
                    Ok(())
                }
                ManifestProgressResult::RunComplete => {
                    // The run is complete, nothing more to do.
                    Ok(())
                }
                ManifestProgressResult::NewProgress {
                    newly_observed_test_index,
                } => {
                    // Enqueue a new wait-for-manifest-progress timeout with the newly-observed
                    // progress.
                    let timeout_spec = timeout_manager
                        .strategy()
                        .wait_for_manifest_progress_duration(newly_observed_test_index);
                    timeout_manager.insert(run_id, timeout_spec).await;

                    Ok(())
                }
            },
            None => {
                illegal_state!(
                    "Invalid state: timeout fired for non-existent run ID",
                    ?run_id
                );
                Ok(())
            }
        }
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
    remote: RemotePersister,
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
    remote: RemotePersister,
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
            remote,
        } = self;
        let ctx = SchedulerCtx {
            queues,
            handshake_ctx: Arc::new(listener.handshake_ctx()),
            persist_manifest,
            remote,
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
                    ctx.remote,
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
                    NotYetPersisted => RetryManifestResponse::NotYetPersisted,
                    Error(e) => RetryManifestResponse::Error(e),
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
        remote: RemotePersister,
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
                    opt_read_error.located(here!())?
                }
            };

            use net_protocol::work_server::NextTestResponse;

            // Pull the next bundle of work.
            let next_work_result = queues.next_work(entity, &run_id);

            let NextWorkResult {
                bundle,
                status,
                opt_persistence,
            } = next_work_result;

            let response = NextTestResponse::Bundle(bundle);

            net_protocol::async_write(&mut conn, &response)
                .await
                .located(here!())?;

            if let Some(PersistAfterManifestDone {
                persist_manifest_plan,
                persist_run_state_plan,
            }) = opt_persistence
            {
                // Our last task will be to execute manifest and run-state persistence.
                let manifest_persistence_task = run_manifest_persistence_task(
                    &persist_manifest,
                    &run_id,
                    persist_manifest_plan,
                    entity,
                );
                let run_state_persistence_task =
                    run_persist_run_state_task(&remote, &run_id, persist_run_state_plan, entity);

                tokio::join!(manifest_persistence_task, run_state_persistence_task);

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

#[instrument(level = "info", skip_all, fields(run_id=?results_persistence.run_id(), entity=?entity))]
async fn run_summary_persistence_task(
    entity: Entity,
    persist_results: &SharedPersistResults,
    results_persistence: ResultsPersistedCell,
    eligible_for_remote_dump: EligibleForRemoteDump,
    summary: results::Summary,
) -> OpaqueResult<()> {
    let task = results_persistence.build_persist_summary_plan(
        persist_results,
        summary,
        eligible_for_remote_dump,
    );

    task.execute().await
}

#[instrument(level = "info", skip_all, fields(run_id=?run_id, entity=?entity))]
async fn run_manifest_persistence_task(
    persist_manifest: &SharedPersistManifest,
    run_id: &RunId,
    persist_manifest_plan: PersistManifestPlan<'_>,
    entity: Entity,
) {
    let task = manifest::make_persistence_task(persist_manifest.borrowed(), persist_manifest_plan);

    if let Err(error) = task.await.entity(entity) {
        log_entityful_error!(error, "failed to execute manifest persistence job: {}");
    }
}

#[instrument(level = "info", skip_all, fields(run_id=?run_id, entity=?entity))]
async fn run_persist_run_state_task(
    remote: &RemotePersister,
    run_id: &RunId,
    persist_run_state: PersistRunStatePlan<'_>,
    entity: Entity,
) {
    if let Err(error) = persist_run_state.persist(remote).await.entity(entity) {
        log_entityful_error!(error, "failed to execute run state persistence job: {}");
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
        Ok(manifest) => RetryManifestResponse::Manifest(NextWorkBundle {
            work: manifest,
            eow: Eow(true),
        }),
        Err(error) => {
            log_entityful_error!(
                error,
                "manifest marked as persisted, but its loading failed: {}"
            );
            RetryManifestResponse::Error(RetryManifestError::FailedToLoad)
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
        persistence::{
            self,
            manifest::InMemoryPersistor,
            remote::{self, LoadedRunState},
            run_state,
        },
        queue::{
            AddedManifest, CancelReason, ManifestProgressCancelReason, ManifestProgressResult,
            PulledTestsStatus, QueueServer, RunStatus, SharedRuns, WorkScheduler,
        },
        timeout::RunTimeoutManager,
    };
    use abq_run_n_times::n_times;
    use abq_test_utils::{
        accept_handshake, assert_scoped_log, build_fake_connection, build_random_strategies,
        one_nonzero_usize, spec,
    };
    use abq_utils::{
        auth::{build_strategies, AdminToken, ClientAuthStrategy, ServerAuthStrategy, UserToken},
        error::ErrorLocation,
        exit::ExitCode,
        here,
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
    use abq_workers::{AssignedRun, AssignedRunStatus};
    use futures::FutureExt;
    use tracing_test::traced_test;

    fn test_queue() -> QueueServer {
        QueueServer::new(
            Default::default(),
            "0.0.0.0:0".parse().unwrap(),
            persistence::results::InMemoryPersistor::new_shared(),
            RunTimeoutManager::default(),
        )
    }

    #[tokio::test]
    #[traced_test]
    async fn bad_message_doesnt_take_down_server() {
        let server = test_queue();

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

        let queue_server = test_queue();
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
            remote: remote::NoopPersister::new().into(),
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
            remote: remote::NoopPersister::new().into(),
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
        let server = test_queue();
        let negotiator_addr = server.public_negotiator_addr;

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
        let server = test_queue();

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
            remote: remote::NoopPersister::new().into(),
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
            remote: remote::NoopPersister::new().into(),
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

    #[tokio::test]
    #[with_protocol_version]
    async fn active_runs_when_some_waiting_on_manifest() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let _ = queues
            .find_or_create_run(
                &run_id,
                one_nonzero_usize(),
                Entity::runner(0, 1),
                &remote::NoopPersister::new().into(),
            )
            .await;

        assert_eq!(queues.estimate_num_active_runs(), 1);
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn active_runs_when_running() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let _ = queues
            .find_or_create_run(
                &run_id,
                one_nonzero_usize(),
                Entity::runner(0, 1),
                &remote::NoopPersister::new().into(),
            )
            .await;
        let added = queues.add_manifest(&run_id, vec![], Default::default());
        assert!(matches!(added, AddedManifest::Added { .. }));

        assert_eq!(queues.estimate_num_active_runs(), 1);
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn active_runs_when_all_done() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let _ = queues
            .find_or_create_run(
                &run_id,
                one_nonzero_usize(),
                Entity::runner(0, 1),
                &remote::NoopPersister::new().into(),
            )
            .await;
        let added = queues.add_manifest(&run_id, vec![], Default::default());
        assert!(matches!(added, AddedManifest::Added { .. }));
        queues.try_mark_reached_end_of_manifest(&run_id, ExitCode::SUCCESS);

        assert_eq!(queues.estimate_num_active_runs(), 0);
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn active_runs_multiple_in_various_states() {
        let queues = SharedRuns::default();

        let run_id1 = RunId::unique(); // DONE
        let run_id2 = RunId::unique(); // DONE
        let run_id3 = RunId::unique(); // WAITING
        let run_id4 = RunId::unique(); // RUNNING
        let expected_active = 2;

        for run_id in [&run_id1, &run_id2, &run_id3, &run_id4] {
            let _ = queues
                .find_or_create_run(
                    run_id,
                    one_nonzero_usize(),
                    Entity::runner(0, 1),
                    &remote::NoopPersister::new().into(),
                )
                .await;
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

    #[tokio::test]
    #[with_protocol_version]
    async fn mark_cancellation_when_some_waiting_on_manifest() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let _ = queues
            .find_or_create_run(
                &run_id,
                one_nonzero_usize(),
                Entity::runner(0, 1),
                &remote::NoopPersister::new().into(),
            )
            .await;

        assert_eq!(queues.estimate_num_active_runs(), 1);

        queues.mark_cancelled(&run_id, Entity::runner(0, 1), CancelReason::User);

        assert_eq!(queues.estimate_num_active_runs(), 0);
        assert!(matches!(
            queues.get_run_status(&run_id).unwrap(),
            RunStatus::Cancelled
        ));
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn mark_cancellation_after_already_done_does_nothing() {
        let queues = SharedRuns::default();
        let remote = remote::NoopPersister::new().into();

        let run_id = RunId::unique();

        let _ = queues
            .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
            .await;
        let _ = queues.add_manifest(&run_id, vec![], Default::default());
        let persist_plan = queues.try_mark_reached_end_of_manifest(&run_id, ExitCode::SUCCESS);
        assert!(persist_plan.is_some());

        queues.mark_cancelled(&run_id, Entity::runner(0, 1), CancelReason::User);

        assert!(matches!(
            queues.get_run_status(&run_id).unwrap(),
            RunStatus::InitialManifestDone {
                num_active_workers: 0
            },
        ));
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn mark_cancellation_when_running() {
        let queues = SharedRuns::default();
        let remote = remote::NoopPersister::new().into();

        let run_id = RunId::unique();

        let _ = queues
            .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
            .await;

        let added = queues.add_manifest(&run_id, vec![], Default::default());
        assert!(matches!(added, AddedManifest::Added { .. }));

        assert_eq!(queues.estimate_num_active_runs(), 1);

        queues.mark_cancelled(&run_id, Entity::runner(0, 1), CancelReason::User);

        assert_eq!(queues.estimate_num_active_runs(), 0);
        assert!(matches!(
            queues.get_run_status(&run_id).unwrap(),
            RunStatus::Cancelled
        ));
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn track_worker_entities_over_run_state() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let worker0 = Entity::runner(1, 1);
        let worker1 = Entity::runner(2, 2);
        let worker2 = Entity::runner(3, 3);

        let remote = remote::NoopPersister::new().into();

        // worker0 creates run
        {
            let assigned_lookup = queues
                .find_or_create_run(&run_id, one_nonzero_usize(), worker0, &remote)
                .await;
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
            let assigned_lookup = queues
                .find_or_create_run(&run_id, one_nonzero_usize(), worker1, &remote)
                .await;
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
            let assigned_lookup = queues
                .find_or_create_run(&run_id, one_nonzero_usize(), worker2, &remote)
                .await;
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

        let remote = remote::NoopPersister::new().into();

        let client = client_opts.build_async().unwrap();

        // Build up our initial state - run is registered, no manifest yet
        let run_id = RunId::unique();
        let queues = {
            let queues = SharedRuns::default();
            let _ = queues
                .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
                .await;
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
                persistence::results::InMemoryPersistor::new_shared(),
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

    #[n_times(1_000)]
    #[tokio::test]
    #[with_protocol_version]
    async fn cancel_and_receive_manifest_concurrently() {
        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let fake_server = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let fake_server_addr = fake_server.local_addr().unwrap();

        let client = client_opts.build_async().unwrap();
        let remote = remote::NoopPersister::new().into();

        // Build up our initial state - run is registered, no manifest yet
        let run_id = RunId::unique();
        let queues = {
            let queues = SharedRuns::default();
            let _ = queues
                .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
                .await;
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
                    persistence::results::InMemoryPersistor::new_shared(),
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

    #[n_times(1_000)]
    #[tokio::test]
    #[with_protocol_version]
    async fn receiving_cancellation_during_last_test_results_is_cancellation() {
        let run_id = RunId::unique();
        let remote = remote::NoopPersister::new().into();
        let queues = {
            let queues = SharedRuns::default();
            let _ = queues
                .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
                .await;
            let added = queues.add_manifest(&run_id, vec![], Default::default());
            assert!(matches!(added, AddedManifest::Added { .. }));
            queues
        };

        let entity = Entity::runner(0, 1);

        let (_listener, server_conn, _client_conn) = build_fake_connection().await;

        let result = AssociatedTestResults::fake(WorkId::new(), vec![TestResult::fake()]);
        let send_last_result_fut = QueueServer::handle_worker_results(
            queues.clone(),
            persistence::results::InMemoryPersistor::new_shared(),
            run_id.clone(),
            entity,
            vec![result],
            server_conn,
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

    #[tokio::test]
    #[with_protocol_version]
    async fn handle_manifest_progress_timeout_while_waiting_for_manifest() {
        let queues = SharedRuns::default();
        let remote = remote::NoopPersister::new().into();

        let run_id = RunId::unique();

        let _ = queues
            .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
            .await;

        let result = queues.handle_manifest_progress_timeout(&run_id, 0);
        assert_eq!(
            result,
            Some(ManifestProgressResult::Cancelled(
                ManifestProgressCancelReason::ManifestNotReceived
            ))
        );
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn handle_manifest_progress_timeout_while_manifest_exists() {
        let queues = SharedRuns::default();
        let remote = remote::NoopPersister::new().into();

        let run_id = RunId::unique();

        let _ = queues
            .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
            .await;
        let _ = queues.add_manifest(&run_id, vec![spec(1)], Default::default());

        let result = queues.handle_manifest_progress_timeout(&run_id, 0);
        assert_eq!(
            result,
            Some(ManifestProgressResult::Cancelled(
                ManifestProgressCancelReason::NoTestProgress
            ))
        );
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn handle_manifest_progress_timeout_when_manifest_progress_made() {
        let queues = SharedRuns::default();
        let remote = remote::NoopPersister::new().into();

        let run_id = RunId::unique();

        let _ = queues
            .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
            .await;
        let _ = queues.add_manifest(&run_id, vec![spec(1), spec(2)], Default::default());
        let _ = queues.next_work(Entity::runner(0, 1), &run_id);

        let result = queues.handle_manifest_progress_timeout(&run_id, 0);
        assert_eq!(
            result,
            Some(ManifestProgressResult::NewProgress {
                newly_observed_test_index: 1
            })
        );
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn handle_manifest_progress_timeout_when_manifest_completed() {
        let queues = SharedRuns::default();
        let remote = remote::NoopPersister::new().into();

        let run_id = RunId::unique();

        let _ = queues
            .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
            .await;
        let _ = queues.add_manifest(&run_id, vec![spec(1)], Default::default());
        let _ = queues.next_work(Entity::runner(0, 1), &run_id);

        let result = queues.handle_manifest_progress_timeout(&run_id, 0);
        assert_eq!(result, Some(ManifestProgressResult::RunComplete));
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn handle_manifest_progress_timeout_when_cancelled() {
        let queues = SharedRuns::default();
        let remote = remote::NoopPersister::new().into();

        let run_id = RunId::unique();

        let _ = queues
            .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
            .await;
        queues.mark_cancelled(&run_id, Entity::runner(0, 1), CancelReason::User);

        let result = queues.handle_manifest_progress_timeout(&run_id, 0);
        assert_eq!(result, Some(ManifestProgressResult::RunComplete));
    }

    #[test]
    #[with_protocol_version]
    fn handle_manifest_progress_timeout_when_does_not_exist() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let result = queues.handle_manifest_progress_timeout(&run_id, 0);
        assert_eq!(result, None);
    }

    #[n_times(1_000)]
    #[tokio::test]
    async fn handle_manifest_progress_timeout_while_run_is_active_races() {
        let queues = SharedRuns::default();
        let remote = remote::NoopPersister::new().into();
        let run_id = RunId::unique();

        let _ = queues
            .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
            .await;
        let _ = queues.add_manifest(&run_id, vec![spec(1), spec(2)], Default::default());

        // Race popping an item off the manifest, and handling the no-progress timeout.
        // If the no-progress handler wins, the run should be cancelled and getting next work
        // should return nothing.
        // If the next-work handler wins, the run should be marked as progressed and the no-progress
        // handler should return RunComplete.

        let manifest_progress_handler = {
            let queues = queues.clone();
            let run_id = run_id.clone();
            move || {
                let result = queues.handle_manifest_progress_timeout(&run_id, 0);
                result.unwrap()
            }
        };

        let next_work_handler = {
            let queues = queues.clone();
            let run_id = run_id.clone();
            move || {
                let result = queues.next_work(Entity::runner(0, 1), &run_id);
                (result.bundle, result.status)
            }
        };

        let manifest_progress_thread;
        let next_work_thread;
        if i % 2 == 0 {
            manifest_progress_thread = std::thread::spawn(manifest_progress_handler);
            next_work_thread = std::thread::spawn(next_work_handler);
        } else {
            next_work_thread = std::thread::spawn(next_work_handler);
            manifest_progress_thread = std::thread::spawn(manifest_progress_handler);
        }

        let (manifest_progress_result, (work_bundle, work_status)) = (
            manifest_progress_thread.join().unwrap(),
            next_work_thread.join().unwrap(),
        );

        match manifest_progress_result {
            ManifestProgressResult::Cancelled(ManifestProgressCancelReason::NoTestProgress) => {
                assert_eq!(work_bundle.work, []);
                assert!(work_bundle.eow);
                assert_eq!(work_status, PulledTestsStatus::QueueWasEmpty);
                assert_eq!(queues.get_run_status(&run_id), Some(RunStatus::Cancelled));
            }
            ManifestProgressResult::NewProgress {
                newly_observed_test_index: 1,
            } => {
                assert_eq!(work_bundle.work.len(), 1);
                assert!(!work_bundle.eow);
                assert_eq!(work_status, PulledTestsStatus::MoreTestsRemaining);
                assert_eq!(queues.get_run_status(&run_id), Some(RunStatus::Active));
            }
            result => unreachable!(
                "invalid combination: {:?}",
                (result, work_bundle, work_status)
            ),
        }
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn create_run_if_no_remote_run_present() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let remote = remote::NoopPersister::new().into();

        let assigned = queues
            .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
            .await;

        assert_eq!(queues.estimate_num_active_runs(), 1);
        assert_eq!(
            assigned,
            AssignedRunStatus::Run(AssignedRun::Fresh {
                should_generate_manifest: true
            })
        );
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn create_run_defers_to_remote_if_present_with_active_worker() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let entity = Entity::runner(0, 1);

        let remote = remote::FakePersister::builder()
            .on_try_load_run_state({
                let expected_run_id = run_id.clone();
                move |run_id| {
                    assert_eq!(run_id, &expected_run_id);
                    let run_state = run_state::RunState {
                        seen_workers: vec![entity],
                        ..run_state::RunState::fake()
                    };
                    async { Ok(LoadedRunState::Found(run_state)) }.boxed()
                }
            })
            .build()
            .into();

        let assigned = queues
            .find_or_create_run(&run_id, one_nonzero_usize(), entity, &remote)
            .await;

        assert_eq!(assigned, AssignedRunStatus::Run(AssignedRun::Retry));
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn create_run_defers_to_remote_if_present_with_inactive_worker() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let remote = remote::FakePersister::builder()
            .on_try_load_run_state({
                let expected_run_id = run_id.clone();
                move |run_id| {
                    assert_eq!(run_id, &expected_run_id);
                    let run_state = run_state::RunState {
                        seen_workers: vec![],
                        new_worker_exit_code: ExitCode::new(77),
                        ..run_state::RunState::fake()
                    };
                    async { Ok(LoadedRunState::Found(run_state)) }.boxed()
                }
            })
            .build()
            .into();

        let assigned = queues
            .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
            .await;

        assert_eq!(
            assigned,
            AssignedRunStatus::AlreadyDone {
                exit_code: ExitCode::new(77)
            }
        );
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn create_run_defers_to_local_if_remote_not_found() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let remote = remote::FakePersister::builder()
            .on_try_load_run_state({
                let expected_run_id = run_id.clone();
                move |run_id| {
                    assert_eq!(run_id, &expected_run_id);
                    async { Ok(LoadedRunState::NotFound) }.boxed()
                }
            })
            .build()
            .into();

        let assigned = queues
            .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
            .await;

        assert_eq!(
            assigned,
            AssignedRunStatus::Run(AssignedRun::Fresh {
                should_generate_manifest: true
            })
        );
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn create_run_defers_to_local_if_remote_schema_version_mismatch() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let remote = remote::FakePersister::builder()
            .on_try_load_run_state({
                let expected_run_id = run_id.clone();
                move |run_id| {
                    assert_eq!(run_id, &expected_run_id);
                    async {
                        Ok(LoadedRunState::IncompatibleSchemaVersion {
                            found: 1,
                            expected: 2,
                        })
                    }
                    .boxed()
                }
            })
            .build()
            .into();

        let assigned = queues
            .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
            .await;

        assert_eq!(
            assigned,
            AssignedRunStatus::Run(AssignedRun::Fresh {
                should_generate_manifest: true
            })
        );
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn create_run_errors_if_remote_check_errors() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let remote = remote::FakePersister::builder()
            .on_try_load_run_state({
                let expected_run_id = run_id.clone();
                move |run_id| {
                    assert_eq!(run_id, &expected_run_id);
                    async { Err("i failed".located(here!())) }.boxed()
                }
            })
            .build()
            .into();

        let assigned = queues
            .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
            .await;

        assert!(matches!(assigned, AssignedRunStatus::FatalError(_)));
    }
}

#[cfg(test)]
mod test_pull_work {
    use abq_test_utils::one_nonzero_usize;
    use abq_utils::net_protocol::{
        entity::Entity,
        workers::{RunId, WorkerTest},
    };
    use abq_with_protocol_version::with_protocol_version;

    use crate::{
        persistence::results::ResultsPersistedCell,
        queue::{
            fake_test_spec, JobQueue, NextWorkResult, PulledTestsStatus, RunState, RunStatus,
            SharedRuns,
        },
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
        let run_id = RunId::unique();

        let has_work = RunState::HasWork {
            queue,
            init_metadata: Default::default(),
            batch_size_hint,
            active_workers: Default::default(),
            results_persistence: ResultsPersistedCell::new(run_id.clone()),
        };

        let queues = SharedRuns::default();
        queues.set_state(run_id.clone(), has_work);

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
                if bundle.work.len() == 1
                && bundle.eow.0
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
                if bundle.work.is_empty()
                && bundle.eow.0
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
mod persistence_on_end_of_manifest {
    use futures::FutureExt;
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use crate::{
        job_queue::JobQueue,
        persistence::{
            manifest::{InMemoryPersistor, SharedPersistManifest},
            remote::{self, RemotePersister},
            results::ResultsPersistedCell,
        },
        queue::{fake_test_spec, NextWorkResult, RunState, SharedRuns, WorkScheduler},
    };
    use abq_test_utils::{one_nonzero_usize, spec};
    use abq_utils::{
        auth::{ClientAuthStrategy, ServerAuthStrategy},
        net_async,
        net_opt::{ClientOptions, ServerOptions},
        net_protocol::{
            self,
            entity::Entity,
            work_server,
            workers::{NextWorkBundle, RunId, WorkerTest, INIT_RUN_NUMBER},
        },
        server_shutdown::ShutdownManager,
        tls::{ClientTlsStrategy, ServerTlsStrategy},
    };
    use abq_with_protocol_version::with_protocol_version;
    use abq_workers::{AssignedRun, AssignedRunStatus};
    use ntest::timeout;
    use parking_lot::Mutex;
    use tokio::task::JoinHandle;

    #[tokio::test]
    #[with_protocol_version]
    async fn worker_told_to_pull_retry_manifest() {
        let queues = SharedRuns::default();
        let remote = remote::NoopPersister::new().into();

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
            let _ = queues
                .find_or_create_run(&run_id, one_nonzero_usize(), worker0, &remote)
                .await;
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
                vec![WorkerTest::new(test1.clone(), INIT_RUN_NUMBER)]
            );
        }

        // Suppose worker0 sporadically dies. Now worker0_shadow should be told to pull a retry
        // manifest.
        let assigned = queues
            .find_or_create_run(&run_id, one_nonzero_usize(), worker0_shadow, &remote)
            .await;
        assert_eq!(assigned, AssignedRunStatus::Run(AssignedRun::Retry));
    }

    struct WorkSchedulerBuilder(WorkScheduler);
    impl WorkSchedulerBuilder {
        fn new(queues: SharedRuns) -> Self {
            Self(WorkScheduler {
                queues,
                persist_manifest: InMemoryPersistor::shared(),
                remote: remote::NoopPersister::new().into(),
            })
        }
        fn with_persist_manifest(mut self, persist: SharedPersistManifest) -> Self {
            self.0.persist_manifest = persist;
            self
        }
        fn with_remote(mut self, remote: RemotePersister) -> Self {
            self.0.remote = remote;
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
    async fn pulling_end_of_manifest_eventually_persists_manifest_and_run_state() {
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
                results_persistence: ResultsPersistedCell::new(run_id.clone()),
            };

            let queues = SharedRuns::default();
            queues.set_state(run_id.clone(), has_work);
            queues
        };

        let persist_manifest = InMemoryPersistor::shared();

        let stored_run_state = Arc::new(Mutex::new(None));
        let remote = remote::FakePersister::builder()
            .on_store_run_state({
                let expected_run_id = run_id.clone();
                let stored_run_state = stored_run_state.clone();
                move |run_id, run_state| {
                    assert_eq!(run_id, &expected_run_id);
                    stored_run_state.lock().replace(run_state);
                    async { Ok(()) }.boxed()
                }
            })
            .build();

        let WorkSchedulerState {
            mut shutdown_tx,
            server_task,
            server_addr,
            mut conn,
            client,
        } = build_work_scheduler(
            WorkSchedulerBuilder::new(queues.clone())
                .with_persist_manifest(persist_manifest.clone())
                .with_remote(remote.into()),
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

        let NextWorkBundle { work, eow } = bundle;

        assert_eq!(
            work,
            vec![
                WorkerTest::new(spec1.clone(), 1),
                WorkerTest::new(spec2.clone(), 1),
            ]
        );
        assert!(eow);

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
                Manifest(NextWorkBundle { work, eow }) => {
                    assert_eq!(
                        work,
                        vec![WorkerTest::new(spec1, 1), WorkerTest::new(spec2, 1),]
                    );
                    assert!(eow);
                    break;
                }
                NotYetPersisted => {
                    tokio::time::sleep(Duration::from_micros(100)).await;
                    continue;
                }
                response => unreachable!("{response:?}"),
            }
        }

        // In time, we should see that the run state was persisted.
        loop {
            if let Some(run_state) = &*stored_run_state.lock() {
                let run_state = run_state.clone().into_run_state();
                assert_eq!(run_state.seen_workers.len(), 1);
                assert_eq!(run_state.new_worker_exit_code.get(), 0);
                break;
            }
            tokio::time::sleep(Duration::from_micros(100)).await;
        }

        shutdown_tx.shutdown_immediately().unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn pull_retry_manifest_for_active_work() {
        let queues = SharedRuns::default();
        let remote = remote::NoopPersister::new().into();
        let run_id = RunId::unique();

        let test1 = fake_test_spec(proto);
        let test2 = fake_test_spec(proto);
        let test3 = fake_test_spec(proto);

        let entity = Entity::runner(0, 1);

        let _ = queues
            .find_or_create_run(&run_id, one_nonzero_usize(), Entity::runner(0, 1), &remote)
            .await;
        let _ = queues.add_manifest(
            &run_id,
            vec![test1.clone(), test2.clone(), test3],
            Default::default(),
        );

        // Prime the queue, pulling two entries for the entity
        {
            let NextWorkResult { bundle, .. } = queues.next_work(entity, &run_id);
            let NextWorkBundle { work, eow } = bundle;
            assert_eq!(work, vec![WorkerTest::new(test1.clone(), INIT_RUN_NUMBER)]);
            assert!(!eow);
        }
        {
            let NextWorkResult { bundle, .. } = queues.next_work(entity, &run_id);
            let NextWorkBundle { work, eow } = bundle;
            assert_eq!(work, vec![WorkerTest::new(test2.clone(), INIT_RUN_NUMBER)]);
            assert!(!eow);
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
            work_server::RetryManifestResponse::Manifest(NextWorkBundle { work, eow }) => {
                assert_eq!(
                    work,
                    vec![
                        WorkerTest::new(test1, INIT_RUN_NUMBER),
                        WorkerTest::new(test2, INIT_RUN_NUMBER),
                    ]
                );
                assert!(eow);
            }
            _ => panic!(),
        }

        shutdown_tx.shutdown_immediately().unwrap();
        server_task.await.unwrap();
    }
}

#[cfg(test)]
mod persist_results {
    use std::time::Instant;

    use abq_test_utils::{build_fake_connection, one_nonzero_usize, wid};
    use abq_utils::{
        exit::ExitCode,
        net_protocol::{
            self,
            entity::{Entity, Tag},
            queue::{AssociatedTestResults, CancelReason, TestResultsResponse},
            results::ResultsLine,
            runners::TestResult,
            workers::RunId,
        },
    };
    use ntest::timeout;

    use crate::{
        job_queue::JobQueue,
        persistence::{self, results::ResultsPersistedCell},
        queue::ReadResultsState,
        worker_tracking::WorkerSet,
    };

    use super::{
        ManifestPersistence, PersistAfterManifestDone, QueueServer, ReadResultsError,
        ResultsPersistence, RetryManifestState, RunState, SharedRuns,
    };

    #[tokio::test]
    #[timeout(1000)]
    async fn sending_results_when_active_eventually_persists_results() {
        let run_id = RunId::unique();
        let results_persistence = persistence::results::InMemoryPersistor::new_shared();
        let results_cell = ResultsPersistedCell::new(run_id.clone());
        let queues = {
            let queues = SharedRuns::default();
            let done = RunState::HasWork {
                queue: JobQueue::default(),
                init_metadata: Default::default(),
                active_workers: Default::default(),
                results_persistence: results_cell.clone(),
                batch_size_hint: one_nonzero_usize(),
            };
            queues.set_state(run_id.clone(), done);
            queues
        };

        let results = vec![AssociatedTestResults::fake(
            wid(1),
            vec![TestResult::fake()],
        )];

        let (_listener, server_conn, _client_conn) = build_fake_connection().await;

        QueueServer::handle_worker_results(
            queues.clone(),
            results_persistence.clone(),
            run_id.clone(),
            Entity::runner(0, 1),
            results.clone(),
            server_conn,
        )
        .await
        .unwrap();

        let opt_retrieved = results_cell.retrieve(&results_persistence).await;
        assert!(opt_retrieved.is_some(), "outstanding pending results");
        let actual_results = opt_retrieved.unwrap().unwrap().decode().unwrap();
        assert_eq!(actual_results, vec![ResultsLine::Results(results)]);
    }

    #[tokio::test]
    #[timeout(1000)]
    async fn sending_results_when_initial_manifest_done_eventually_persists_results() {
        let run_id = RunId::unique();
        let results_persistence = persistence::results::InMemoryPersistor::new_shared();
        let results_cell = ResultsPersistedCell::new(run_id.clone());
        let queues = {
            let queues = SharedRuns::default();
            let has_work = RunState::InitialManifestDone {
                new_worker_exit_code: ExitCode::SUCCESS,
                init_metadata: Default::default(),
                seen_workers: Default::default(),
                manifest_persistence: ManifestPersistence::EmptyManifest,
                results_persistence: ResultsPersistence::Persisted(results_cell.clone()),
            };
            queues.set_state(run_id.clone(), has_work);
            queues
        };

        let results = vec![AssociatedTestResults::fake(
            wid(1),
            vec![TestResult::fake()],
        )];

        let (_listener, server_conn, _client_conn) = build_fake_connection().await;

        QueueServer::handle_worker_results(
            queues.clone(),
            results_persistence.clone(),
            run_id.clone(),
            Entity::runner(0, 1),
            results.clone(),
            server_conn,
        )
        .await
        .unwrap();

        let opt_retrieved = results_cell.retrieve(&results_persistence).await;
        assert!(opt_retrieved.is_some(), "outstanding pending results");
        let actual_results = opt_retrieved.unwrap().unwrap().decode().unwrap();
        assert_eq!(actual_results, vec![ResultsLine::Results(results)]);
    }

    #[tokio::test]
    #[timeout(1000)]
    async fn sending_results_when_cancelled_is_error_and_does_nothing() {
        let run_id = RunId::unique();
        let results_persistence = persistence::results::InMemoryPersistor::new_shared();
        let results_cell = ResultsPersistedCell::new(run_id.clone());
        let queues = {
            let queues = SharedRuns::default();
            let done = RunState::Cancelled {
                reason: CancelReason::User,
            };
            queues.set_state(run_id.clone(), done);
            queues
        };

        let results = vec![AssociatedTestResults::fake(
            wid(1),
            vec![TestResult::fake()],
        )];

        let (_listener, server_conn, _client_conn) = build_fake_connection().await;

        let result = QueueServer::handle_worker_results(
            queues.clone(),
            results_persistence.clone(),
            run_id.clone(),
            Entity::runner(0, 1),
            results.clone(),
            server_conn,
        )
        .await;

        assert!(result.is_err());

        let opt_retrieved = results_cell.retrieve(&results_persistence).await;
        assert!(opt_retrieved.is_some(), "outstanding pending results");
        assert!(
            opt_retrieved.unwrap().is_err(),
            "cancelled run before any results has results associated"
        );
    }

    macro_rules! get_read_results_cell {
        ($test:ident, $state:expr, $($expect_match:tt)+) => {
            #[test]
            fn $test() {
                let run_id = RunId::unique();
                let queues = SharedRuns::default();
                queues.set_state(run_id.clone(), $state);
                let result = queues.get_read_results_cell(&run_id);
                assert!(matches!(&result, $($expect_match)*), "{result:?}");
            }
        };
    }

    get_read_results_cell! {
        get_read_results_cell_when_waiting_for_manifest,
        RunState::WaitingForManifest { worker_connection_times: Default::default(), batch_size_hint: one_nonzero_usize() },
        Err(ReadResultsError::WaitingForManifest)
    }

    get_read_results_cell! {
        get_read_results_cell_when_has_work,
        {
            let mut active_workers = WorkerSet::with_capacity(1);
            active_workers.insert_by_tag(Entity::runner(1, 1), None);
            active_workers.insert_by_tag(Entity::runner(2, 1), Some(Instant::now()));
            RunState::HasWork {
                queue: Default::default(),
                init_metadata: Default::default(),
                batch_size_hint: one_nonzero_usize(),
                active_workers: parking_lot::Mutex::new(active_workers),
                results_persistence: ResultsPersistedCell::new(RunId::unique()),
            }
        },
        Ok(ReadResultsState::OutstandingRunners(r)) if r == &[Tag::runner(1, 1)]
    }

    get_read_results_cell! {
        get_read_results_cell_when_done_no_pending_workers,
        {
            let mut active_workers = WorkerSet::with_capacity(1);
            active_workers.insert_by_tag(Entity::runner(1, 1), false);
            active_workers.insert_by_tag(Entity::runner(2, 1), false);
            RunState::InitialManifestDone {
                new_worker_exit_code: ExitCode::SUCCESS,
                init_metadata: Default::default(),
                seen_workers: parking_lot::RwLock::new(active_workers),
                results_persistence: ResultsPersistence::Persisted(ResultsPersistedCell::new(RunId::unique())),
                manifest_persistence: ManifestPersistence::EmptyManifest,
            }
        },
        Ok(ReadResultsState::ReadFromCell(..))
    }

    get_read_results_cell! {
        get_read_results_cell_when_done_with_pending_workers,
        {
            let mut active_workers = WorkerSet::with_capacity(1);
            active_workers.insert_by_tag(Entity::runner(1, 1), false);
            active_workers.insert_by_tag(Entity::runner(2, 1), true);
            RunState::InitialManifestDone {
                new_worker_exit_code: ExitCode::SUCCESS,
                init_metadata: Default::default(),
                seen_workers: parking_lot::RwLock::new(active_workers),
                results_persistence: ResultsPersistence::Persisted(ResultsPersistedCell::new(RunId::unique())),
                manifest_persistence: ManifestPersistence::EmptyManifest,
            }
        },
        Ok(ReadResultsState::OutstandingRunners(r)) if r == &[Tag::runner(2, 1)]
    }

    get_read_results_cell! {
        get_read_results_cell_when_done_with_manifest_never_received,
        {
            RunState::InitialManifestDone {
                new_worker_exit_code: ExitCode::SUCCESS,
                init_metadata: Default::default(),
                seen_workers: Default::default(),
                results_persistence: ResultsPersistence::ManifestNeverReceived,
                manifest_persistence: ManifestPersistence::EmptyManifest,
            }
        },
        Err(ReadResultsError::ManifestNeverReceived)
    }

    get_read_results_cell! {
        get_read_results_cell_when_cancelled,
        {
            RunState::Cancelled {
                reason: CancelReason::User
            }
        },
        Err(ReadResultsError::RunCancelled)
    }

    #[tokio::test]
    #[timeout(1000)]
    async fn fetching_results_after_results_provided_and_persisted_multiple() {
        let run_id = RunId::unique();
        let results_persistence = persistence::results::InMemoryPersistor::new_shared();
        let manifest_persistence = persistence::manifest::InMemoryPersistor::shared();
        let results_cell = ResultsPersistedCell::new(run_id.clone());
        let queues = {
            let queues = SharedRuns::default();
            let batch_size_hint = one_nonzero_usize();
            // Pretend that worker 1, runner 1 has already finished.
            let mut active_workers = WorkerSet::with_capacity(1);
            active_workers.insert_by_tag(Entity::runner(1, 1), Some(Instant::now()));
            let has_work = RunState::HasWork {
                queue: JobQueue::default(),
                batch_size_hint,
                init_metadata: Default::default(),
                active_workers: Default::default(),
                results_persistence: results_cell.clone(),
            };
            queues.set_state(run_id.clone(), has_work);
            queues
        };

        let results1 = vec![AssociatedTestResults::fake(
            wid(1),
            vec![TestResult::fake()],
        )];

        let results2 = vec![AssociatedTestResults::fake(
            wid(2),
            vec![TestResult::fake()],
        )];

        for results in [results1.clone(), results2.clone()] {
            let (_listener, server_conn, _client_conn) = build_fake_connection().await;
            QueueServer::handle_worker_results(
                queues.clone(),
                results_persistence.clone(),
                run_id.clone(),
                Entity::runner(0, 1),
                results,
                server_conn,
            )
            .await
            .unwrap();
        }

        // Move into a completed state
        let PersistAfterManifestDone {
            persist_manifest_plan,
            persist_run_state_plan: _,
        } = queues
            .try_mark_reached_end_of_manifest(&run_id, ExitCode::SUCCESS)
            .unwrap();
        persistence::manifest::make_persistence_task(
            manifest_persistence.borrowed(),
            persist_manifest_plan,
        )
        .await
        .unwrap();

        // Setup simulation of fetching results.
        let get_test_results_response = {
            let run_id = run_id.clone();
            let queues = queues.clone();
            let results_persistence = results_persistence.clone();
            move || {
                let run_id = run_id.clone();
                let queues = queues.clone();
                let results_persistence = results_persistence.clone();
                async move {
                    let (_listener, server_conn, mut client_conn) = build_fake_connection().await;

                    let fetch_results_fut = async move {
                        QueueServer::handle_test_results_request(
                            queues,
                            results_persistence,
                            run_id,
                            Entity::local_client(),
                            server_conn,
                        )
                        .await
                        .unwrap()
                    };

                    let read_results_fut = async move {
                        net_protocol::async_read::<_, TestResultsResponse>(&mut client_conn)
                            .await
                            .unwrap()
                    };

                    let ((), response) = tokio::join!(fetch_results_fut, read_results_fut);

                    response
                }
            }
        };

        use TestResultsResponse::*;

        // Fetch the state of the results after the initial manifest has been completed.
        {
            let expected_results = vec![
                ResultsLine::Results(results1.clone()),
                ResultsLine::Results(results2.clone()),
            ];
            let response = get_test_results_response().await;
            match response {
                Results {
                    chunk: results,
                    final_chunk: true,
                } => {
                    let results = results.decode().unwrap();
                    assert_eq!(results, expected_results);
                }
                response => unreachable!("{response:?}"),
            }
        }

        // Suppose that worker 1, runner 1 re-attaches for retry. We shouldn't be allowed to fetch
        // results.
        {
            let retry_manifest = queues.get_retry_manifest(&run_id, Entity::runner(1, 1));
            assert!(
                matches!(retry_manifest, RetryManifestState::FetchFromPersistence),
                "{retry_manifest:?}"
            );
            let response = get_test_results_response().await;
            match response {
                OutstandingRunners(tags) => {
                    assert_eq!(tags, vec![Tag::runner(1, 1)]);
                }
                response => unreachable!("{response:?}"),
            }
        }

        // Simulate the runner returning a third set of results and then completing.
        // After the worker completes, we should be able to fetch the results again.
        {
            let results3 = vec![AssociatedTestResults::fake(
                wid(3),
                vec![TestResult::fake()],
            )];

            let (_listener, server_conn, _client_conn) = build_fake_connection().await;

            QueueServer::handle_worker_results(
                queues.clone(),
                results_persistence.clone(),
                run_id.clone(),
                Entity::runner(1, 1),
                results3.clone(),
                server_conn,
            )
            .await
            .unwrap();

            queues.mark_worker_complete(&run_id, Entity::runner(1, 1), Instant::now());

            let expected_results = vec![
                ResultsLine::Results(results1),
                ResultsLine::Results(results2),
                ResultsLine::Results(results3),
            ];
            let response = get_test_results_response().await;
            match response {
                Results {
                    chunk: results,
                    final_chunk: true,
                } => {
                    let results = results.decode().unwrap();
                    assert_eq!(results, expected_results);
                }
                response => unreachable!("{response:?}"),
            }
        }
    }
}
