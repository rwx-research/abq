use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU64;
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::{io, time};

use abq_utils::auth::ServerAuthStrategy;
use abq_utils::exit::ExitCode;
use abq_utils::net;
use abq_utils::net_async::{self, UnverifiedServerStream};
use abq_utils::net_opt::ServerOptions;
use abq_utils::net_protocol::entity::Entity;
use abq_utils::net_protocol::queue::{
    AssociatedTestResults, CancelReason, InvokerTestData, NativeRunnerInfo, NegotiatorInfo,
    Request, RunStartData, TestSpec,
};
use abq_utils::net_protocol::runners::{CapturedOutput, MetadataMap};
use abq_utils::net_protocol::work_server;
use abq_utils::net_protocol::workers::{
    ManifestResult, NextWorkBundle, ReportedManifest, WorkerTest, INIT_RUN_NUMBER,
};
use abq_utils::net_protocol::{
    self,
    queue::{InvokeWork, Message},
    workers::{NextWork, RunId},
};
use abq_utils::net_protocol::{client, meta, publicize_addr};
use abq_utils::shutdown::{RetirementCell, ShutdownManager, ShutdownReceiver};
use abq_utils::tls::ServerTlsStrategy;
use abq_utils::{atomic, log_assert};
use abq_workers::negotiate::{
    AssignedRun, AssignedRunStatus, QueueNegotiator, QueueNegotiatorHandle,
};

use parking_lot::{Mutex, RwLock};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;
use tracing::instrument;

use crate::active_state::{ActiveRunState, RunStateCache};
use crate::attempts::AttemptsCounter;
use crate::connections::{self, ConnectedWorkers};
use crate::prelude::*;
use crate::supervisor_responder::{BufferedResults, SupervisorResponder};
use crate::timeout::{RunTimeoutManager, RunTimeoutStrategy, TimedOutRun, TimeoutReason};
use crate::worker_timings::{
    log_workers_idle_after_completion_latency, log_workers_waited_for_manifest_latency,
    new_worker_timings, WorkerTimings,
};

#[derive(Default, Debug)]
struct JobQueue {
    queue: VecDeque<WorkerTest>,
}

impl JobQueue {
    fn add_batch_work(&mut self, work: impl IntoIterator<Item = WorkerTest>) {
        self.queue.extend(work.into_iter());
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn get_work(&mut self, n: NonZeroU64) -> impl Iterator<Item = WorkerTest> + '_ {
        // We'll give back the number of tests that were asked for, or everything if there aren't
        // that many tests left.
        let chop_off = std::cmp::min(self.queue.len(), n.get() as usize);
        self.queue.drain(..chop_off)
    }
}

#[derive(Debug)]
enum RunState {
    /// Supervisor has not yet connected, but workers may be asking about this run.
    WaitingForSupervisor {
        /// For the purposes of analytics, records timings of when workers connect prior to the
        /// test run being kicked off by a supervisor.
        worker_connection_times: WorkerTimings,
    },
    /// Supervisor has connected, but first worker has not connected for manifest generation.
    WaitingForFirstWorker {
        /// For the purposes of analytics, records timings of when workers connect prior to the
        /// manifest being generated.
        worker_connection_times: WorkerTimings,
    },
    /// Supervisor and first worker has connected. Waiting for manifest.
    WaitingForManifest {
        /// For the purposes of analytics, records timings of when workers connect prior to the
        /// manifest being generated.
        worker_connection_times: WorkerTimings,
    },
    /// The active state of the test suite run. The queue is populated and a supervisor, worker is
    /// connected.
    HasWork {
        queue: JobQueue,

        /// Top-level test suite metadata workers should initialize with.
        init_metadata: MetadataMap,

        /// The number of tests to batch to a worker at a time, as hinted by an invoker of the work.
        // Co-locate this here so that we don't have to index `run_data` when grabbing a
        // test.
        batch_size_hint: NonZeroU64,

        /// The timeout for the last test result.
        last_test_timeout: Duration,

        /// How many attempts of the test suite have been made.
        /// The test suite is seen as complete when the end of the queue is at the max attempt number.
        attempts_counter: AttemptsCounter,
    },
    Done,
    Cancelled {
        #[allow(unused)] // yet
        reason: CancelReason,
    },
}

struct RunData {
    /// The number of tests to batch to a worker at a time, as hinted by an invoker of the work.
    batch_size_hint: NonZeroU64,
    /// The timeout for the last test result.
    last_test_timeout: Duration,
    /// The maximum attempt number of the test suite. Always >= [INIT_RUN_NUMBER]
    max_attempt_no: u32,
}

// Just the stack size of the RunData, the closure of RunData may be much larger.
static_assertions::assert_eq_size!(RunData, (u64, Duration, bool));
static_assertions::assert_eq_size!(Option<RunData>, RunData);

const MAX_BATCH_SIZE: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(100) };

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
    /// Representation:
    ///   - read/write lock to insert and access runs. Insertion of runs is monotonically increasing,
    ///     and reading of run state is far more common than writing a new run.
    ///   - mutex to access run data. Writes are far more common during an active test run as
    ///     workers move the active test pointer.
    ///     TODO: switch to RwLock for #185
    runs: RwLock<
        //
        HashMap<RunId, Mutex<Run>>,
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

/// A reason a new queue for a run could not be created.
#[derive(Debug)]
enum NewQueueError {
    /// The run ID has an active run.
    RunIdAlreadyInProgress,
    /// The run ID was previously completed.
    RunIdPreviouslyCompleted,
}

enum AssignedRunLookup {
    Some(AssignedRun),
    /// There is no associated run yet known.
    NotFound,
    /// An associated run is known, but it has already completed; a worker should exit immediately.
    AlreadyDone {
        exit_code: ExitCode,
    },
}

#[derive(Debug, Clone, Copy)]
enum RunLive {
    Active,
    Done,
    Cancelled,
}

enum InitMetadata {
    Metadata(MetadataMap),
    WaitingForManifest,
    RunAlreadyCompleted,
}

/// What is the state of the queue of tests for a run after some bundle of tests have been pulled?
#[derive(Clone, Copy)]
enum PulledTestsStatus {
    /// There are additional tests remaining.
    MoreTestsRemaining,
    /// The bundle of tests just pulled have exhausted the queue of tests.
    /// The last test to be handed out is in this bundle.
    PulledLastTest { last_test_timeout: Duration },
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
enum NextWorkResult {
    /// A set of tests were found.
    Present {
        bundle: NextWorkBundle,
        status: PulledTestsStatus,
    },
    /// Tests will be available, but they are not yet known.
    Pending,
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

impl AllRuns {
    /// Attempts to create a new queue for a run.
    /// If the given run ID already has an associated queue, an error is returned.
    pub fn create_queue(
        &self,
        run_id: RunId,
        batch_size_hint: NonZeroU64,
        max_run_attempt: u32,
        last_test_timeout: Duration,
    ) -> Result<(), NewQueueError> {
        // Take an exclusive lock over the run state for the entirety of the update.
        let mut runs = self.runs.write();

        let worker_timings;
        match runs.get(&run_id) {
            Some(state) => match &mut state.lock().state {
                RunState::WaitingForFirstWorker { .. }
                | RunState::WaitingForManifest { .. }
                | RunState::HasWork { .. } => return Err(NewQueueError::RunIdAlreadyInProgress),
                RunState::Done { .. } | RunState::Cancelled { .. } => {
                    return Err(NewQueueError::RunIdPreviouslyCompleted)
                }
                RunState::WaitingForSupervisor {
                    worker_connection_times,
                } => worker_timings = std::mem::take(worker_connection_times),
            },
            None => {
                worker_timings = new_worker_timings();
            }
        }

        // NB: Always add first for conversative estimation.
        self.num_active.fetch_add(1, atomic::ORDERING);

        // The run ID is fresh; create a new queue for it.
        let run = Run {
            state: RunState::WaitingForFirstWorker {
                worker_connection_times: worker_timings,
            },
            data: Some(RunData {
                batch_size_hint,
                last_test_timeout,
                max_attempt_no: max_run_attempt,
            }),
        };
        let old_run = runs.insert(run_id, Mutex::new(run));

        debug_assert!(
            old_run.is_none()
                || matches!(old_run, Some(e) if matches!(e.lock().state, RunState::WaitingForSupervisor {..}))
        );

        Ok(())
    }

    // Chooses a run for a set of workers to attach to. Returns `None` if an appropriate run is not
    // found.
    pub fn find_run_for_worker(&self, run_id: &RunId, worker_entity: Entity) -> AssignedRunLookup {
        {
            if self.runs.read().get(run_id).is_none() {
                let mut runs = self.runs.write();
                // Possible TOCTOU race here, so we must check whether the run is still missing
                // before we insert a waiting-for-supervisor marker.
                // If the run state is now populated, defer to that, since it can only
                // monotonically advance the state machine (run states move forward and are
                // never removed).
                if !runs.contains_key(run_id) {
                    let run = Run {
                        state: RunState::WaitingForSupervisor {
                            worker_connection_times: new_worker_timings(),
                        },
                        data: None,
                    };
                    runs.insert(run_id.clone(), Mutex::new(run));
                }
            };
        }

        let runs = self.runs.read();
        let mut run = match runs.get(run_id) {
            Some(st) => st.lock(),
            None => {
                tracing::error!(
                    ?run_id,
                    "illegal state - a run must always be populated when looking up for worker"
                );
                return AssignedRunLookup::NotFound;
            }
        };

        if run.data.is_none() {
            // If there is an active run, then the run data exists iff the run state exists;
            // however, if the run state is known to be complete, the run data will already
            // have been pruned, and the worker should not be given a run.
            match &mut run.state {
                RunState::Done => {
                    // The worker should exit successfully locally, regardless of what the overall
                    // code was.
                    return AssignedRunLookup::AlreadyDone {
                        exit_code: ExitCode::SUCCESS,
                    };
                }
                RunState::Cancelled { .. } => {
                    return AssignedRunLookup::AlreadyDone {
                        exit_code: ExitCode::CANCELLED,
                    }
                }
                RunState::WaitingForSupervisor {
                    worker_connection_times,
                } => {
                    if !worker_connection_times.contains(&worker_entity) {
                        worker_connection_times.insert(worker_entity, time::Instant::now());
                    }
                    return AssignedRunLookup::NotFound;
                }
                st => {
                    tracing::error!(
                        run_state=?st,
                        "run data is missing, but run state is present and not marked as Done"
                    );
                    unreachable!(
                        "run state should only be `Done` if data is missing, but it was {:?}",
                        st
                    );
                }
            }
        };

        let assigned_run = match &mut run.state {
            RunState::WaitingForFirstWorker {
                worker_connection_times,
            } => {
                // This is the first worker to attach; ask it to generate the manifest.
                run.state = RunState::WaitingForManifest {
                    worker_connection_times: std::mem::take(worker_connection_times),
                };
                AssignedRun {
                    run_id: run_id.clone(),
                    should_generate_manifest: true,
                }
            }
            // Otherwise we are already waiting for the manifest, or already have it; just tell
            // the worker what runner it should set up.
            RunState::WaitingForManifest {
                worker_connection_times,
            } => {
                // Record the time this worker connected, if this is its first time.
                if !worker_connection_times.contains(&worker_entity) {
                    worker_connection_times.insert(worker_entity, time::Instant::now());
                }
                AssignedRun {
                    run_id: run_id.clone(),
                    should_generate_manifest: false,
                }
            }
            _ => AssignedRun {
                run_id: run_id.clone(),
                should_generate_manifest: false,
            },
        };

        AssignedRunLookup::Some(assigned_run)
    }

    /// Adds the initial manifest for an ABQ test suite run.
    pub fn add_manifest(
        &self,
        run_id: &RunId,
        flat_manifest: Vec<TestSpec>,
        init_metadata: MetadataMap,
    ) -> AddedManifest {
        let runs = self.runs.read();

        let mut run = runs.get(run_id).expect("no run recorded").lock();

        let worker_connection_times = match &mut run.state {
            RunState::WaitingForManifest {
                worker_connection_times,
            } => {
                // expected state, pass through
                std::mem::take(worker_connection_times)
            }
            RunState::WaitingForSupervisor { .. }
            | RunState::WaitingForFirstWorker { .. }
            | RunState::HasWork { .. }
            | RunState::Done { .. } => {
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

        let mut queue = JobQueue::default();
        queue.add_batch_work(work_from_manifest);

        let run_data = run
            .data
            .as_ref()
            .expect("illegal state - run data must exist at this step");

        run.state = RunState::HasWork {
            queue,
            batch_size_hint: run_data.batch_size_hint,
            init_metadata,
            last_test_timeout: run_data.last_test_timeout,
            attempts_counter: AttemptsCounter::new(run_data.max_attempt_no),
        };

        AddedManifest::Added {
            worker_connection_times,
        }
    }

    pub fn init_metadata(&self, run_id: RunId) -> InitMetadata {
        let runs = self.runs.read();

        let run = runs.get(&run_id).expect("no run recorded").lock();

        match &run.state {
            RunState::WaitingForSupervisor { .. } => {
                tracing::error!("illegal state - worker may not request initialization metadata until run is known");
                InitMetadata::WaitingForManifest
            }
            RunState::WaitingForFirstWorker { .. } | RunState::WaitingForManifest { .. } => {
                InitMetadata::WaitingForManifest
            }
            RunState::HasWork { init_metadata, .. } => {
                InitMetadata::Metadata(init_metadata.clone())
            }
            RunState::Done { .. } | RunState::Cancelled { .. } => InitMetadata::RunAlreadyCompleted,
        }
    }

    pub fn next_work(&self, run_id: &RunId) -> NextWorkResult {
        let runs = self.runs.read();

        let mut run = runs.get(run_id).expect("no run recorded").lock();

        match &mut run.state
        {
            RunState::HasWork {
                queue,
                batch_size_hint,
                last_test_timeout,
                attempts_counter,
                ..
            } => {
                Self::determine_next_work(queue, *batch_size_hint, *last_test_timeout, attempts_counter)
            }
             RunState::Done { .. } | RunState::Cancelled {..} => {
                // Let the worker know that we've reached the end of the queue.
                NextWorkResult::Present {
                    bundle: NextWorkBundle::new(vec![NextWork::EndOfWork]),
                    status: PulledTestsStatus::QueueWasEmpty
                }
            }
            RunState::WaitingForSupervisor { .. } |
            RunState::WaitingForFirstWorker {..} |
            RunState::WaitingForManifest {..} => unreachable!("Invalid state - work can only be requested after initialization metadata, at which point the manifest is known.")
        }
    }

    fn determine_next_work(
        queue: &mut JobQueue,
        batch_size_hint: NonZeroU64,
        last_test_timeout: Duration,
        attempts_counter: &mut AttemptsCounter,
    ) -> NextWorkResult {
        // NB: in the future, the batch size is likely to be determined intelligently, i.e.
        // from off-line timing data. But for now, we use the hint the client provided.
        let batch_size = batch_size_hint;

        // Pop the next batch.
        let mut bundle = Vec::with_capacity(batch_size.get() as _);
        bundle.extend(queue.get_work(batch_size).map(NextWork::Work));

        let pulled_tests_status;

        if queue.is_empty() {
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
                Some(NextWork::Work(test)) => {
                    // We just pulled the last test for the given attempt.
                    // If that was also the last attempt, we're all done. Otherwise, we will
                    // eventually have more attempts to run.
                    attempts_counter.account_last_of_attempt(test.run_number);

                    if attempts_counter.completed_all_attempts() {
                        pulled_tests_status =
                            PulledTestsStatus::PulledLastTest { last_test_timeout };
                    } else {
                        pulled_tests_status = PulledTestsStatus::MoreTestsRemaining;
                    }
                }
                Some(NextWork::EndOfWork) => {
                    // To avoid additional allocation and moves when we pop the queue, we built
                    // `bundle` as NextWork above, but it only consists of `NextWork::Work` variants.
                    unreachable!(
                        "illegal state - EndOfWork variant is not available at this point."
                    )
                }
                None => {
                    if attempts_counter.completed_all_attempts() {
                        pulled_tests_status = PulledTestsStatus::QueueWasEmpty;
                    } else {
                        // There are more tests in
                        return NextWorkResult::Pending;
                    }
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

        NextWorkResult::Present {
            bundle,
            status: pulled_tests_status,
        }
    }

    pub fn mark_complete(&self, run_id: &RunId) {
        let runs = self.runs.read();

        let mut run = runs.get(run_id).expect("no run recorded").lock();

        match &mut run.state {
            RunState::HasWork { queue, .. } => {
                assert!(queue.is_empty(), "Invalid state - queue is not complete!");
            }
            RunState::Cancelled { .. } => {
                // Cancellation always takes priority over completeness.
                return;
            }
            RunState::WaitingForSupervisor { .. }
            | RunState::WaitingForFirstWorker { .. }
            | RunState::WaitingForManifest { .. }
            | RunState::Done { .. } => {
                unreachable!("Invalid state");
            }
        }

        run.state = RunState::Done;

        // Drop the run data, since we no longer need it.
        debug_assert!(run.data.is_some());
        std::mem::take(&mut run.data);

        // NB: Always sub last for conversative estimation.
        self.num_active.fetch_sub(1, atomic::ORDERING);
    }

    pub fn mark_failed_to_start(&self, run_id: RunId) {
        let runs = self.runs.read();

        let mut run = runs.get(&run_id).expect("no run recorded").lock();

        match run.state {
            RunState::WaitingForManifest { .. } => {
                // okay
            }
            RunState::Cancelled { .. } => {
                // No-op, since the run was already cancelled.
                return;
            }
            RunState::WaitingForSupervisor { .. }
            | RunState::WaitingForFirstWorker { .. }
            | RunState::HasWork { .. }
            | RunState::Done { .. } => {
                unreachable!("Invalid state - can only fail to start while waiting for manifest");
            }
        }

        run.state = RunState::Done;

        // Drop the run data, since we no longer need it.
        debug_assert!(run.data.is_some());
        std::mem::take(&mut run.data);

        // NB: Always sub last for conversative estimation.
        self.num_active.fetch_sub(1, atomic::ORDERING);
    }

    /// Marks a run as complete because it had the trivial manifest.
    pub fn mark_empty_manifest_complete(&self, run_id: RunId) {
        let runs = self.runs.read();

        let mut run = runs.get(&run_id).expect("no run recorded").lock();

        match run.state {
            RunState::WaitingForManifest { .. } => {
                // okay
            }
            RunState::Cancelled { .. } => {
                // No-op, since the run was already cancelled.
                return;
            }
            RunState::WaitingForSupervisor { .. }
            | RunState::WaitingForFirstWorker { .. }
            | RunState::HasWork { .. }
            | RunState::Done { .. } => {
                unreachable!("Invalid state - can only mark complete due to manifest while waiting for manifest");
            }
        }

        // Trivially successful
        run.state = RunState::Done;

        // Drop the run data, since we no longer need it.
        debug_assert!(run.data.is_some());
        std::mem::take(&mut run.data);

        // NB: Always sub last for conversative estimation.
        self.num_active.fetch_sub(1, atomic::ORDERING);
    }

    pub fn mark_cancelled(&self, run_id: &RunId, reason: CancelReason) {
        let runs = self.runs.read();

        let mut run = runs.get(run_id).expect("no run recorded").lock();

        // Cancellation can happen at any time, including after the test run is determined to have
        // completed with a particular success status by one party, because that observation may
        // not have been shared with a test supervisor prior to their cancellation of a test run.
        //
        // We prefer the observation of the test supervisor, so test cancellation is always marked
        // as a failure.
        run.state = RunState::Cancelled { reason };

        // Drop the run data if it exists, since we no longer need it.
        // It might already be missing if this cancellation happened after we saw a run complete.
        std::mem::take(&mut run.data);

        // NB: Always sub last for conversative estimation.
        self.num_active.fetch_sub(1, atomic::ORDERING);
    }

    fn get_run_liveness(&self, run_id: &RunId) -> Option<RunLive> {
        let runs = self.runs.read();

        let run = runs.get(run_id)?.lock();

        match run.state {
            RunState::WaitingForSupervisor { .. } => None,
            RunState::WaitingForFirstWorker { .. }
            | RunState::WaitingForManifest { .. }
            | RunState::HasWork { .. } => Some(RunLive::Active),
            RunState::Done => Some(RunLive::Done),
            RunState::Cancelled { .. } => Some(RunLive::Cancelled),
        }
    }

    fn estimate_num_active_runs(&self) -> u64 {
        self.num_active.load(atomic::ORDERING)
    }

    #[cfg(test)]
    fn set_state(&self, run_id: RunId, state: RunState) -> Option<Mutex<Run>> {
        let mut runs = self.runs.write();
        runs.insert(run_id, Mutex::new(Run { state, data: None }))
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
    Io(#[from] io::Error),
}

impl Abq {
    pub fn server_addr(&self) -> SocketAddr {
        self.server_addr
    }

    pub fn work_server_addr(&self) -> SocketAddr {
        self.work_scheduler_addr
    }

    pub fn start(config: QueueConfig) -> Self {
        start_queue(config)
    }

    pub fn wait_forever(&mut self) {
        self.server_handle.take().map(JoinHandle::join);
        self.work_scheduler_handle.take().map(JoinHandle::join);
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
    pub fn shutdown(&mut self) -> Result<(), AbqError> {
        debug_assert!(self.active);

        self.active = false;

        // Shut down all of our servers.
        self.shutdown_manager.shutdown_immediately()?;
        self.negotiator.join();

        self.server_handle
            .take()
            .expect("server handle must be available during shutdown")
            .join()
            .unwrap()?;

        self.work_scheduler_handle
            .take()
            .expect("worker handle must be available during shutdown")
            .join()
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
    /// How timeouts for long-lasting runs should be configured.
    pub timeout_strategy: RunTimeoutStrategy,
}

impl Default for QueueConfig {
    /// Creates a [`QueueConfig`] that always binds and advertises on INADDR_ANY, with arbitrary
    /// ports for its servers.
    fn default() -> Self {
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
            timeout_strategy: RunTimeoutStrategy::RUN_BASED,
        }
    }
}

/// Initializes a queue, binding the queue negotiator to the given address.
/// All other public channels to the queue (the work and results server) are exposed
/// on the same host IP address as the negotiator is.
fn start_queue(config: QueueConfig) -> Abq {
    let QueueConfig {
        public_ip,
        bind_ip,
        server_port,
        work_port,
        negotiator_port,
        server_options,
        timeout_strategy,
    } = config;

    let mut shutdown_manager = ShutdownManager::default();

    let queues: SharedRuns = Default::default();

    let server_listener = server_options.clone().bind((bind_ip, server_port)).unwrap();
    let server_addr = server_listener.local_addr().unwrap();

    let negotiator_listener = server_options
        .clone()
        .bind((bind_ip, negotiator_port))
        .unwrap();
    let negotiator_addr = negotiator_listener.local_addr().unwrap();
    let public_negotiator_addr = publicize_addr(negotiator_addr, public_ip);

    let timeout_manager = RunTimeoutManager::new(timeout_strategy);
    let worker_next_tests_tasks = ConnectedWorkers::default();

    let server_shutdown_rx = shutdown_manager.add_receiver();
    let server_handle = thread::spawn({
        let queue_server = QueueServer::new(queues.clone(), public_negotiator_addr);
        let timeout_manager = timeout_manager.clone();
        let worker_next_tests_tasks = worker_next_tests_tasks.clone();
        move || {
            queue_server.start_on(
                server_listener,
                timeout_manager,
                server_shutdown_rx,
                worker_next_tests_tasks,
            )
        }
    });

    let new_work_server = server_options.bind((bind_ip, work_port)).unwrap();
    let new_work_server_addr = new_work_server.local_addr().unwrap();

    let work_scheduler_shutdown_rx = shutdown_manager.add_receiver();
    let work_scheduler_handle = thread::spawn({
        let scheduler = WorkScheduler {
            queues: queues.clone(),
        };
        move || {
            scheduler.start_on(
                new_work_server,
                timeout_manager,
                work_scheduler_shutdown_rx,
                worker_next_tests_tasks,
            )
        }
    });

    // Provide the execution context a set of workers should attach with, if it is known at the
    // time of polling. We must not to block here, since that can take a lock over the shared
    // queues indefinitely.
    let choose_run_for_worker = {
        let queues = queues.clone();
        move |wanted_run_id: &RunId, entity: Entity| {
            let opt_assigned = { queues.find_run_for_worker(wanted_run_id, entity) };
            match opt_assigned {
                AssignedRunLookup::Some(assigned) => AssignedRunStatus::Run(assigned),
                AssignedRunLookup::NotFound => AssignedRunStatus::RunUnknown,
                AssignedRunLookup::AlreadyDone { exit_code } => {
                    AssignedRunStatus::AlreadyDone { exit_code }
                }
            }
        }
    };

    let negotiator_shutdown_rx = shutdown_manager.add_receiver();
    let negotiator = QueueNegotiator::new(
        public_ip,
        negotiator_listener,
        negotiator_shutdown_rx,
        new_work_server_addr,
        server_addr,
        choose_run_for_worker,
    )
    .unwrap();

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

/// run ID -> result responder
type ActiveRunRespondersInner = HashMap<RunId, tokio::sync::Mutex<SupervisorResponder>>;

/// Shared responders.
type ActiveRunResponders = Arc<
    tokio::sync::RwLock<
        //
        ActiveRunRespondersInner,
    >,
>;

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

/// An error that occurs when an abq client attempts to re-connect to the queue.
#[derive(Debug, Error, PartialEq)]
enum ClientReconnectionError {
    /// The client sent an initial test run request to the queue.
    #[error("{0} was never used to invoke work on the queue")]
    NeverInvoked(RunId),
}

#[derive(Clone)]
struct QueueServerCtx {
    queues: SharedRuns,
    /// When all results for a particular run are communicated, we want to make sure that the
    /// responder thread is closed and that the entry here is dropped.
    active_runs: ActiveRunResponders,
    state_cache: RunStateCache,
    public_negotiator_addr: SocketAddr,
    handshake_ctx: Arc<Box<dyn net_async::ServerHandshakeCtx>>,

    /// Persisted worker connections yielding test results.
    /// A task is spawned when a worker opens a persistent connection with the
    /// queue to incrementally stream test results, and the task simply listens
    /// for new test result messages until the worker notifies completion.
    worker_results_tasks: ConnectedWorkers,

    /// Persisted worker connections that fetch new tests to run.
    /// These connections are only created on request to the [WorkScheduler],
    /// and are only [shutdown][ConnectedWorkers::stop] when a test run completes.
    /// As such, we maintain a pointer so we can stop all remaining tasks when we receive a final
    /// test result.
    worker_next_tests_tasks: ConnectedWorkers,

    /// Holds state on whether the queue is retired, and records retirement if the queue is asked
    /// to retire by a privileged process.
    retirement: RetirementCell,
}

impl QueueServer {
    fn new(queues: SharedRuns, public_negotiator_addr: SocketAddr) -> Self {
        Self {
            queues,
            public_negotiator_addr,
        }
    }

    fn start_on(
        self,
        server_listener: Box<dyn net::ServerListener>,
        timeouts: RunTimeoutManager,
        mut shutdown: ShutdownReceiver,
        worker_next_tests_tasks: ConnectedWorkers,
    ) -> Result<(), QueueServerError> {
        // Create a new tokio runtime solely for handling requests that come into the queue server.
        //
        // Note that all of the work done by the queue is purely coordination, so we're heavily I/O
        // bound. As such we put all this work on one thread.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        // Start the server in the tokio runtime.
        let server_result: Result<(), QueueServerError> = rt.block_on(async {
            // Initialize the async server listener.
            let server_listener = server_listener.into_async()?;

            let Self {
                queues,
                public_negotiator_addr,
            } = self;

            let ctx = QueueServerCtx {
                queues,
                active_runs: Default::default(),
                state_cache: Default::default(),
                public_negotiator_addr,
                handshake_ctx: Arc::new(server_listener.handshake_ctx()),

                worker_results_tasks: Default::default(),
                worker_next_tests_tasks,

                retirement: shutdown.get_retirement_cell(),
            };

            enum Task {
                HandleConn(UnverifiedServerStream),
                HandleTimeout(TimedOutRun),
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
                    fired_timeout = timeouts.next_timeout() => {
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
                                log_entityful_error!(
                                    error,
                                    "error handling connection to queue: {}"
                                )
                            }
                        });
                    }
                    HandleTimeout(timeout) => {
                        let ctx = ctx.clone();
                        tokio::spawn(async move {
                            let result = Self::handle_fired_timeout(
                                ctx.queues,
                                ctx.active_runs,
                                ctx.state_cache,
                                timeout,
                            )
                            .await;
                            if let Err(LocatedError {
                                error,
                                location: Location { file, line, column },
                            }) = result
                            {
                                tracing::error!(
                                    file,
                                    line,
                                    column,
                                    "error handling timeout of run: {}",
                                    error
                                );
                            }
                        });
                    }
                }
            }

            Ok(())
        });

        server_result
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
            Message::InvokeWork(invoke_work) => {
                Self::handle_invoked_work(
                    ctx.active_runs,
                    ctx.queues,
                    ctx.retirement,
                    entity,
                    stream,
                    invoke_work,
                )
                .await
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
                // Supervisor connection might exit without FIN ACK - allow disconnect here.
                let _shutdown = stream.shutdown().await;
                drop(stream);

                Self::handle_run_cancellation(
                    ctx.queues,
                    ctx.active_runs,
                    ctx.state_cache,
                    entity,
                    run_id,
                )
                .await
            }
            Message::Reconnect(run_id) => {
                Self::handle_invoker_reconnection(ctx.active_runs, entity, stream, run_id).await
            }
            Message::ManifestResult(run_id, manifest_result) => {
                Self::handle_manifest_result(
                    ctx.queues,
                    ctx.active_runs,
                    ctx.state_cache,
                    entity,
                    run_id,
                    manifest_result,
                    stream,
                )
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
                Self::handle_worker_results(
                    ctx.queues,
                    ctx.active_runs,
                    ctx.state_cache,
                    ctx.worker_results_tasks,
                    ctx.worker_next_tests_tasks,
                    entity,
                    run_id,
                    results,
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
                    ctx.state_cache,
                    run_id,
                    entity,
                    notification_time,
                )
                .await
            }

            Message::PersistentWorkerResultsConnection(run_id) => {
                Self::handle_new_persistent_worker_results_connection(
                    ctx.worker_results_tasks,
                    run_id,
                    entity,
                    stream,
                )
                .await
            }

            Message::RequestTotalRunResult(run_id) => {
                Self::handle_total_run_result_request(ctx.queues, run_id, entity, stream).await
            }

            Message::Retire => Self::handle_retirement(ctx.retirement, entity, stream).await,
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

    #[instrument(level = "trace", skip(active_runs, queues, stream))]
    async fn handle_invoked_work(
        active_runs: ActiveRunResponders,
        queues: SharedRuns,
        retirement: RetirementCell,
        entity: Entity,
        mut stream: Box<dyn net_async::ServerStream>,
        invoke_work: InvokeWork,
    ) -> OpaqueResult<()> {
        Self::reject_if_retired(&entity, retirement).located(here!())?;

        let InvokeWork {
            run_id,
            batch_size_hint,
            max_run_attempt,
            test_results_timeout,
        } = invoke_work;

        tracing::info!(
            ?entity,
            ?run_id,
            batch_size_hint,
            test_results_timeout_seconds = test_results_timeout.as_secs(),
            max_run_attempt,
            "new invoked work"
        );

        let batch_size_hint = if batch_size_hint > MAX_BATCH_SIZE {
            tracing::warn!(
                ?run_id,
                ?batch_size_hint,
                ?MAX_BATCH_SIZE,
                "invocation parameters exceed max batch size"
            );
            MAX_BATCH_SIZE
        } else {
            batch_size_hint
        };

        let could_create_queue = queues.create_queue(
            run_id.clone(),
            batch_size_hint,
            max_run_attempt,
            test_results_timeout,
        );

        match could_create_queue {
            Ok(()) => {
                {
                    tracing::info!(?run_id, ?entity, "created new queue");

                    // Only after telling the client that we are ready for it to move into listing
                    // for test results, associate the results responder.
                    //
                    // NB: an exclusive lock here is important! As unlikely as it may be, sharing the
                    // responder before the response message is sent may result in test results being
                    // sent *before* this response message is sent.
                    let mut active_runs = active_runs.write().await;

                    // TODO: if this write fails, we will be in an inconsistent state and have leaked a
                    // run ID - there will be an active queue, but no active responder for when test
                    // results start streaming back in.
                    // If this run fails, we should switch the queue state to a mode where we say
                    // "something has went wrong" and disallow workers to pull tests until either the
                    // invoker re-invokes the run, or the run gets deleted.
                    net_protocol::async_write(
                        &mut stream,
                        &net_protocol::queue::InvokeWorkResponse::Success,
                    )
                    .await
                    .located(here!())?;

                    // TODO: we could determine a more optimal sizing of the test results buffer by
                    // e.g. considering the size of the test manifest.
                    let results_buffering_size = BufferedResults::DEFAULT_SIZE;

                    let results_responder =
                        SupervisorResponder::new(stream, results_buffering_size);
                    let old_responder =
                        active_runs.insert(run_id, tokio::sync::Mutex::new(results_responder));
                    debug_assert!(
                        old_responder.is_none(),
                        "Existing stream for expected unique run id"
                    );
                }

                Ok(())
            }
            Err(err) => {
                tracing::info!(
                    ?run_id,
                    ?entity,
                    "failed to create new queue for invocation"
                );

                let failure_reason = match err {
                    NewQueueError::RunIdAlreadyInProgress => {
                        net_protocol::queue::InvokeFailureReason::DuplicateRunId {
                            recently_completed: false,
                        }
                    }
                    NewQueueError::RunIdPreviouslyCompleted => {
                        net_protocol::queue::InvokeFailureReason::DuplicateRunId {
                            recently_completed: true,
                        }
                    }
                };
                let failure_msg = net_protocol::queue::InvokeWorkResponse::Failure(failure_reason);

                net_protocol::async_write(&mut stream, &failure_msg)
                    .await
                    .located(here!())?;

                Ok(())
            }
        }
    }

    /// A supervisor cancels a test run.
    #[instrument(level = "trace", skip(queues, active_runs, state_cache))]
    async fn handle_run_cancellation(
        queues: SharedRuns,
        active_runs: ActiveRunResponders,
        state_cache: RunStateCache,
        entity: Entity,
        run_id: RunId,
    ) -> OpaqueResult<()> {
        tracing::info!(?run_id, ?entity, "test supervisor cancelled a test run");

        // IFTTT: handle_manifest_*, handle_worker_results
        //
        // Take an exclusive lock over active_runs for the duration of the update so there is no
        // interference with updating other state.
        let mut active_runs = active_runs.write().await;

        {
            // The supervisor does not need any extra information, so drop the responder.
            active_runs.remove(&run_id);
        }

        {
            // Mark the cancellation in the queue first, so that new queries from workers will be
            // told to terminate.
            queues.mark_cancelled(&run_id, CancelReason::User);
        }

        {
            // Drop the entry from the state cache
            state_cache.lock().remove(&run_id);
        }

        Ok(())
    }

    #[instrument(level = "trace", skip(active_runs, new_stream))]
    async fn handle_invoker_reconnection(
        active_runs: ActiveRunResponders,
        entity: Entity,
        new_stream: Box<dyn net_async::ServerStream>,
        run_id: RunId,
    ) -> OpaqueResult<()> {
        use std::collections::hash_map::Entry;

        // When an abq client loses connection with the queue, they may attempt to reconnect.
        // We'll need to update the connection from streaming results to the client to the new one.
        let mut active_runs = active_runs.write().await;
        let active_conn_entry = active_runs.entry(run_id.clone());
        match active_conn_entry {
            Entry::Occupied(mut occupied) => {
                let new_stream_addr = new_stream.peer_addr();

                occupied
                    .get_mut()
                    .lock()
                    .await
                    .update_connection_to(new_stream)
                    .await;

                tracing::debug!(
                    "rerouted connection for {:?} to {:?}",
                    run_id,
                    new_stream_addr
                );

                Ok(())
            }
            Entry::Vacant(_) => {
                // There was never a connection for this run ID!
                Err(ClientReconnectionError::NeverInvoked(run_id)).located(here!())
            }
        }
    }

    #[instrument(
        level = "trace",
        skip(queues, active_runs, state_cache, manifest_result, stream)
    )]
    async fn handle_manifest_result(
        queues: SharedRuns,
        active_runs: ActiveRunResponders,
        state_cache: RunStateCache,
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
                        active_runs,
                        state_cache,
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
                        active_runs,
                        state_cache,
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
                    active_runs,
                    state_cache,
                    entity,
                    run_id,
                    Err((error, output)),
                    stream,
                )
                .await
            }
        }
    }

    #[instrument(level = "trace", skip(queues, state_cache, flat_manifest))]
    async fn handle_manifest_success(
        queues: SharedRuns,
        active_runs: ActiveRunResponders,
        state_cache: RunStateCache,
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

        // IFTTT: handle_run_cancellation
        //
        // Take an exclusive lock on the active run responder so that no one
        // (including cancellation) can steal it away before the manifest is registered,
        // and the initialization data is communicated.
        //
        // This avoids blocking start of workers for the test run, but will force
        // consumption of the test-start information before any test results.
        let active_runs = active_runs.read().await;
        let opt_responder = match active_runs.get(&run_id) {
            None => None,
            Some(responder) => Some(responder.lock().await),
        };

        let total_work = flat_manifest.len();
        let added_manifest = queues.add_manifest(&run_id, flat_manifest.clone(), init_metadata);

        // Since the supervisor doesn't need to know about the native runner that'll be running the
        // current test suite as soon as the test run starts, only let it know about the runner
        // after we've unblocked the workers.
        match added_manifest {
            AddedManifest::Added {
                worker_connection_times,
            } => {
                let run_state = ActiveRunState::new(total_work);
                state_cache.lock().insert(run_id.clone(), run_state);

                let mut responder = opt_responder
                    .expect("illegal state - run is active, but supervisor responder is missing");

                responder
                    .send_run_start_info(RunStartData {
                        native_runner_info,
                        manifest: flat_manifest,
                    })
                    .await;

                log_workers_waited_for_manifest_latency(
                    &run_id,
                    worker_connection_times,
                    manifest_received_time,
                );
            }
            AddedManifest::RunCancelled => {
                // If the run was already cancelled, there is nothing for us to do.
                assert!(!active_runs.contains_key(&run_id));
                tracing::info!(?run_id, ?entity, "received manifest for cancelled run");
            }
        }

        Ok(())
    }

    #[instrument(level = "trace", skip(queues, state_cache))]
    async fn handle_manifest_empty_or_failure(
        queues: SharedRuns,
        active_runs: ActiveRunResponders,
        state_cache: RunStateCache,
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

        {
            // We should not have an entry for the run in the state cache, since we only ever know
            // the initial state of a run after receiving the manifest.
            debug_assert!(!state_cache.lock().contains_key(&run_id));
        }

        let mut responder = {
            // Remove and take over the responder; since the manifest is not known, we should be the
            // only task that ever communicates with the client.
            let responder = active_runs.write().await.remove(&run_id).unwrap();
            responder.into_inner()
        };

        // Tell the client that the tests are done or that the test runners have failed, as
        // appropriate.
        let final_message = match manifest_result {
            Ok(native_runner_info) => {
                // The native runner info must arrive before any notification that the supervisor
                // should close.
                responder
                    .send_run_start_info(RunStartData {
                        native_runner_info,
                        manifest: vec![],
                    })
                    .await;

                InvokerTestData::EndOfResults
            }
            Err((opaque_manifest_generation_error, captured_output)) => {
                InvokerTestData::TestCommandError {
                    error: opaque_manifest_generation_error,
                    captured: captured_output,
                }
            }
        };

        match responder {
            SupervisorResponder::DirectStream { mut stream, buffer } => {
                debug_assert!(buffer.is_empty(), "messages before manifest received");

                net_protocol::async_write(&mut stream, &final_message)
                    .await
                    .located(here!())?;

                drop(stream);
            }
            SupervisorResponder::Disconnected {
                results,
                buffer_size: _,
                pending_start_info: _,
            } => {
                // Unfortunately, since we still don't have an active connection to the abq
                // client, we can't do anything here.
                // To avoid leaking memory we assume the client is dead and drop the results.
                //
                // TODO: rather than dropping the failure message, consider attaching it back
                // to the queue. If the client re-connects, we'll let them know that the test
                // has failed to startup.
                let _ = results;
            }
        }

        if is_empty_manifest {
            queues.mark_empty_manifest_complete(run_id);
        } else {
            queues.mark_failed_to_start(run_id);
        }

        Ok(())
    }

    #[instrument(
        level = "trace",
        skip(
            queues,
            active_runs,
            state_cache,
            worker_results_tasks,
            worker_next_tests_tasks,
        )
    )]
    async fn handle_worker_results(
        queues: SharedRuns,
        active_runs: ActiveRunResponders,
        state_cache: RunStateCache,
        worker_results_tasks: ConnectedWorkers,
        worker_next_tests_tasks: ConnectedWorkers,
        entity: Entity,
        run_id: RunId,
        results: Vec<AssociatedTestResults>,
    ) -> OpaqueResult<()> {
        // IFTTT: handle_fired_timeout

        tracing::info!("got result");

        let num_results = results.len();

        {
            // First up, chuck the test result back over to `abq test`. Make sure we don't steal
            // the lock on active_runs for any longer than it takes to send that request.
            let active_runs = active_runs.read().await;

            let invoker_stream = match active_runs.get(&run_id) {
                Some(stream) => stream,
                None => {
                    tracing::info!(
                        ?run_id,
                        "got test result for no-longer active (cancelled) run"
                    );
                    return Err("run no longer active").located(here!());
                }
            };

            invoker_stream.lock().await.send_test_results(results).await;
        }

        let no_more_work = {
            // Update the amount of work we have left; again, steal the map of work for only as
            // long is it takes for us to do that.
            let mut state_cache = state_cache.lock();
            let run_state = match state_cache.get_mut(&run_id) {
                Some(state) => state,
                None => {
                    tracing::info!(
                        ?run_id,
                        "got test result for no-longer active (cancelled) run"
                    );
                    return Err("run no longer active").located(here!());
                }
            };

            run_state.account_results(num_results)
        };

        if !no_more_work {
            return Ok(());
        }

        Self::handle_end_of_work(
            queues,
            active_runs,
            state_cache,
            worker_results_tasks,
            worker_next_tests_tasks,
            entity,
            run_id,
        )
        .await
    }

    #[instrument(
        level = "trace",
        skip(
            queues,
            active_runs,
            state_cache,
            worker_results_tasks,
            worker_next_tests_tasks,
        )
    )]
    async fn handle_end_of_work(
        queues: SharedRuns,
        active_runs: ActiveRunResponders,
        state_cache: RunStateCache,
        worker_results_tasks: ConnectedWorkers,
        worker_next_tests_tasks: ConnectedWorkers,
        entity: Entity,
        run_id: RunId,
    ) -> OpaqueResult<()> {
        // Now, we need to notify the end of the test run and handle resource cleanup.
        let run_complete_span = tracing::info_span!("run end of work", ?run_id, ?entity);
        let _run_complete_enter = run_complete_span.enter();

        // First up, exclusively grab our supervisor responder. We can't have anyone else talk to
        // the supervisor until we determine whether the run should keep going or shutdown.
        //
        // We don't need an exclusive lock to update the active_runs yet, but we do take a read
        // guard to prevent cancellation during this path.
        //
        // IFTTT: handle_run_cancellation - cancellation requires an exclusive lock over all active
        // runs.
        let active_runs_guard = active_runs.read().await;
        let responder = active_runs_guard.get(&run_id);
        let responder = match responder {
            Some(responder) => responder,
            None => {
                // The run must have been cancelled between the time we handled the last result,
                // and now when we've started the completion procedure.
                assert!(matches!(
                    queues.get_run_liveness(&run_id),
                    Some(RunLive::Cancelled)
                ));
                tracing::info!(?run_id, "run cancelled while handling run completion");
                return Ok(());
            }
        };
        let mut responder = responder.lock().await;

        responder.flush_results().await;

        match &mut *responder {
            SupervisorResponder::DirectStream { stream, buffer } => {
                assert!(
                    buffer.is_empty(),
                    "no results should be buffered after drainage"
                );

                net_protocol::async_write(stream, &InvokerTestData::EndOfResults)
                    .await
                    .located(here!())?;
                let response: client::EndOfResultsResponse =
                    net_protocol::async_read(stream).await.located(here!())?;

                match response {
                    client::EndOfResultsResponse::AckEnd { exit_code } => {
                        // The supervisor may have closed the socket immediately acknowledgement,
                        // so our shutdown failing is not an error.
                        let _ = stream.shutdown().await;
                        drop(responder);

                        // Release the read guard over the active runs; the supervisor has just
                        // confirmed completion, so re-entry will not need to worry about
                        // cancellation.
                        // XREF: drop-active-run-after-confirmation
                        drop(active_runs_guard);

                        tracing::info!(?run_id, "closed supervisor responder on ACK run end");

                        Self::handle_test_run_completion(
                            queues,
                            active_runs,
                            state_cache,
                            &run_id,
                            entity,
                            exit_code,
                            worker_results_tasks,
                            worker_next_tests_tasks,
                        )
                        .await
                    }
                }
            }
            SupervisorResponder::Disconnected {
                results: _,
                buffer_size: _,
                pending_start_info: _,
            } => {
                tracing::warn!(?run_id, "dropping disconnected results responder");

                // Unfortunately, since we still don't have an active connection to the abq
                // client, we can't send any buffered test results or end-of-tests anywhere.
                // To avoid leaking memory we assume the client is dead and drop the results.
                //
                // TODO: rather than dropping any pending results, consider attaching any
                // pending results back to the queue. If the client re-connects, send them all
                // back at that point. The queue can run a weep to cleanup any uncolleted
                // results on some time interval.
                drop(responder);
                drop(active_runs_guard);

                // Suppose failure, since the supervisor is disconnected.
                let exit_code = ExitCode::FAILURE;

                Self::handle_test_run_completion(
                    queues,
                    active_runs,
                    state_cache,
                    &run_id,
                    entity,
                    exit_code,
                    worker_results_tasks,
                    worker_next_tests_tasks,
                )
                .await
            }
        }
    }

    #[instrument(
        level = "trace",
        skip(
            queues,
            active_runs,
            state_cache,
            worker_results_tasks,
            worker_next_tests_tasks,
        )
    )]
    async fn handle_test_run_completion(
        queues: SharedRuns,
        active_runs: ActiveRunResponders,
        state_cache: RunStateCache,
        run_id: &RunId,
        entity: Entity,
        exit_code: ExitCode,
        worker_results_tasks: ConnectedWorkers,
        worker_next_tests_tasks: ConnectedWorkers,
    ) -> OpaqueResult<()> {
        // This run should indeed be terminated; drop all remaining state.

        // The supervisor has acknowledged exit by this branch, so we can now exclusively
        // drop the responder without worry of TOCTOU due to cancellation.
        // XREF: drop-active-run-after-confirmation
        active_runs.write().await.remove(run_id);

        let run_state = state_cache.lock().remove(run_id);

        queues.mark_complete(run_id);

        let (opt_worker_results_tasks_err, opt_worker_next_tests_tasks_err) = tokio::join!(
            Self::shutdown_persisted_worker_connection_tasks(
                worker_results_tasks,
                run_id,
                "failed to successfully stop worker results connection task",
            ),
            Self::shutdown_persisted_worker_connection_tasks(
                worker_next_tests_tasks,
                run_id,
                "failed to successfully stop worker next tests connection task",
            )
        );

        match run_state {
            Some(state) => {
                log_workers_idle_after_completion_latency(
                    run_id,
                    state.worker_completed_times(),
                    entity,
                );
            }
            None => {
                tracing::error!(?run_id, "run state missing for active run upon completion");
            }
        }

        opt_worker_results_tasks_err.or(opt_worker_next_tests_tasks_err)
    }

    #[instrument(level = "trace", skip(tasks, fail_msg))]
    async fn shutdown_persisted_worker_connection_tasks(
        tasks: ConnectedWorkers,
        run_id: &RunId,
        fail_msg: &str,
    ) -> OpaqueResult<()> {
        match tasks.stop(run_id).await {
            connections::StopResult::RunNotAssociated => {
                // TODO: if we hit this state, it should be an error once we migrate to
                // persisted connections!
                Ok(())
            }
            connections::StopResult::Stopped(errors) if errors.is_empty() => Ok(()),
            connections::StopResult::Stopped(mut errors) => {
                for LocatedError {
                    error,
                    location: Location { file, line, column },
                } in errors.iter()
                {
                    tracing::error!(file, line, column, ?error, ?run_id, "{}", fail_msg);
                }
                Err(errors.remove(0)).located(here!())
            }
        }
    }

    /// Handles fired timeouts for test results, instuted by a [RunTimeoutManager].
    /// A timeout is always fired for a test, whether it completes or not. There are two cases:
    ///
    /// - The test run completed (successfully or not). In this case the firing of the timeout is
    ///   benign.
    /// - The test run is still pending, i.e. we have not yet received all test results. In this
    ///   case the timeout is material, and we mark the test run as failed due to timeout.
    #[instrument(level = "trace", skip(queues, active_runs, state_cache))]
    async fn handle_fired_timeout(
        queues: SharedRuns,
        active_runs: ActiveRunResponders,
        state_cache: RunStateCache,
        timeout: TimedOutRun,
    ) -> OpaqueResult<()> {
        // IFTTT: handle_worker_results
        //
        // We must now take exclusive access on `active_runs` - since that is the first thing that
        // handle_worker_results will lock on, our taking a lock ensures that any test runs that
        // come in after this timeout will not race.
        // We must keep this lock alive until the end of this function.
        let mut locked_active_runs = active_runs.write().await;

        let TimedOutRun {
            run_id,
            after,
            reason,
        } = timeout;

        // NB: taking a persistent lock on the queue or state cache is not necessary,
        // since we've already guarded against other modification above.
        // By not blocking the queue we allow other work to continue freely.
        match queues.get_run_liveness(&run_id) {
            Some(liveness) => match (liveness, reason) {
                (RunLive::Active, TimeoutReason::ResultNotReceived) => {
                    // pass through, the run needs to be timed out.
                }
                (RunLive::Done {}, _) | (RunLive::Cancelled, _) => {
                    return Ok(()); // run was already complete, disregard the timeout
                }
            },
            None => {
                tracing::error!(
                    ?run_id,
                    "timeout for run was fired, but it's not known in the queue"
                );
                return Err("run not known in the queue").located(here!());
            }
        }

        tracing::info!(?run_id, timeout=?after, "timing out active test run");

        // Remove the responder, and notify the supervisor that this run has timed out.
        let opt_responder = locked_active_runs.remove(&run_id);

        Self::check_responder_after_timeout_precondition(&run_id, opt_responder.is_some(), reason);

        if let Some(responder) = opt_responder {
            let mut responder = responder.into_inner();

            // Flush all test results still pending prior to the timeout.
            responder.flush_results().await;

            match responder {
                SupervisorResponder::DirectStream { mut stream, buffer } => {
                    assert!(
                        buffer.is_empty(),
                        "no results should be buffered after drainage"
                    );

                    net_protocol::async_write(&mut stream, &InvokerTestData::TimedOut { after })
                        .await
                        .located(here!())?;

                    // To avoid a race between perceived cancellation by the supervisor and perceived
                    // timeout by the queue, require an ACK for the induced timeout notification. If
                    // instead the supervisor cancels prior to an ACK, this stream will break before
                    // the ACK is fulfilled, and the cancellation will be recorded.
                    let response: client::EndOfResultsResponse =
                        net_protocol::async_read(&mut stream)
                            .await
                            .located(here!())?;

                    log_assert!(
                        matches!(response, client::EndOfResultsResponse::AckEnd { .. }),
                        "supervisor may only acknowledge completion after a timeout"
                    );

                    drop(stream);

                    tracing::info!(?run_id, "closed connected results responder");
                }
                SupervisorResponder::Disconnected {
                    results: _,
                    buffer_size: _,
                    pending_start_info: _,
                } => {
                    tracing::error!(
                        ?run_id,
                        "dropping disconnected results responder after timeout"
                    );

                    // Unfortunately, nothing more we can do at this point, since
                    // the supervisor has been lost!
                }
            }
        }

        // Store the cancellation state
        queues.mark_cancelled(&run_id, reason.into());

        // Drop the run from the state cache.
        state_cache.lock().remove(&run_id);

        let _ = locked_active_runs; // make sure the lock isn't dropped before here

        Ok(())
    }

    fn check_responder_after_timeout_precondition(
        run_id: &RunId,
        responder_present: bool,
        timeout_reason: TimeoutReason,
    ) {
        if matches!(
            (responder_present, timeout_reason),
            (false, TimeoutReason::ResultNotReceived)
        ) {
            tracing::error!(
                ?run_id,
                ?timeout_reason,
                responder_present,
                "illegal state - run responder not aligned with liveness"
            );
        }
    }

    #[instrument(level = "trace", skip(state_cache))]
    async fn handle_worker_ran_all_tests_notification(
        state_cache: RunStateCache,
        run_id: RunId,
        entity: Entity,
        notification_time: time::Instant,
    ) -> OpaqueResult<()> {
        let old_notification = {
            match state_cache.lock().get_mut(&run_id) {
                Some(active_run) => active_run.insert_worker_completed(entity, notification_time),
                None => {
                    // The active state may be missing in a few reasonable situations:
                    // - For the worker that sends the ran-all-tests notification after it sends
                    //   the last test result, since we steal the active state during completion of the
                    //   test run. Note that in that case, the ran-all-tests notification for the
                    //   relevant worker is assumed to be at the moment the test result in question is
                    //   returned.
                    // - Workers that finish around the same time as the final test result's worker. In
                    //   this case, their completion notification can race with the completion of the
                    //   test run.
                    //
                    // In both those situations, workers have completed near the time the whole suite
                    // completed, and we gain little by tracking their time deltas. To avoid slowing
                    // down the overall test suite, and because we don't know exactly how many workers
                    // we may need to wait around for, admit notifications that hit such races.
                    tracing::info!(
                        ?run_id,
                        ?entity,
                        "worker-ran-all-tests notification after test run seen as completed"
                    );
                    return Ok(());
                }
            }
        };

        log_assert!(
            old_notification.is_none(),
            ?run_id,
            ?entity,
            "duplicate worker-ran-all-tests notification received"
        );

        tracing::debug!(?run_id, ?entity, "worker-ran-all-tests notification");

        Ok(())
    }

    async fn handle_new_persistent_worker_results_connection(
        worker_results_tasks: ConnectedWorkers,
        run_id: RunId,
        _entity: Entity,
        _stream: Box<dyn net_async::ServerStream>,
    ) -> OpaqueResult<()> {
        worker_results_tasks.insert(run_id, |rx_stop| async move {
            rx_stop.await.located(here!())?;
            Ok(())
        });

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
            let liveness = queues
                .get_run_liveness(&run_id)
                .ok_or("Invalid state: worker is requesting run result for non-existent ID")
                .located(here!())?;

            match liveness {
                RunLive::Active => net_protocol::queue::TotalRunResult::Pending,
                RunLive::Done => {
                    tracing::info!(?run_id, ?entity, "notifying worker of run exit code");
                    net_protocol::queue::TotalRunResult::Completed
                }
                RunLive::Cancelled => net_protocol::queue::TotalRunResult::Completed,
            }
        };

        net_protocol::async_write(&mut stream, &response)
            .await
            .located(here!())?;

        Ok(())
    }

    #[instrument(level = "trace", skip(stream))]
    async fn handle_retirement(
        retirement: RetirementCell,
        entity: Entity,
        mut stream: Box<dyn net_async::ServerStream>,
    ) -> OpaqueResult<()> {
        if !stream.role().is_admin() {
            tracing::warn!(?entity, "rejecting underprivileged request for retirement");
            return Err("rejecting underprivileged request for retirement").located(here!());
        }

        retirement.notify_asked_to_retire();

        net_protocol::async_write(&mut stream, &net_protocol::queue::AckRetirement {})
            .await
            .located(here!())?;

        Ok(())
    }

    fn reject_if_retired(entity: &Entity, retirement: RetirementCell) -> io::Result<()> {
        if !retirement.is_retired() {
            return Ok(());
        }

        tracing::warn!(
            ?entity,
            "rejecting request not permissible during retirement"
        );
        Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "queue is retiring",
        ))
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
    timeouts: RunTimeoutManager,
    handshake_ctx: Arc<Box<dyn net_async::ServerHandshakeCtx>>,

    /// Persisted worker connections yielding requests for new tests to run.
    /// A task is spawned when a worker opens a persistent connection with the
    /// work server to incrementally stream test results, and the task listens
    /// and responds to new messages until work has been exhausted.
    worker_next_tests_tasks: ConnectedWorkers,
}

impl WorkScheduler {
    fn start_on(
        self,
        listener: Box<dyn net::ServerListener>,
        timeouts: RunTimeoutManager,
        mut shutdown: ShutdownReceiver,
        worker_next_tests_tasks: ConnectedWorkers,
    ) -> Result<(), WorkSchedulerError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let server_result: Result<(), WorkSchedulerError> = rt.block_on(async {
            let listener = listener.into_async()?;

            let Self { queues } = self;
            let ctx = SchedulerCtx {
                queues,
                timeouts,
                handshake_ctx: Arc::new(listener.handshake_ctx()),

                worker_next_tests_tasks,
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
                            log_entityful_error!(
                                error,
                                "error handling connection to work server: {}"
                            )
                        }
                    }
                });
            }

            Ok(())
        });

        server_result
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
                let init_metadata = { ctx.queues.init_metadata(run_id.clone()) };

                use net_protocol::work_server::{InitContext, InitContextResponse};
                let response = match init_metadata {
                    InitMetadata::Metadata(init_meta) => {
                        InitContextResponse::InitContext(InitContext { init_meta })
                    }
                    InitMetadata::RunAlreadyCompleted => InitContextResponse::RunAlreadyCompleted,
                    InitMetadata::WaitingForManifest => InitContextResponse::WaitingForManifest,
                };

                net_protocol::async_write(&mut stream, &response)
                    .await
                    .located(here!())
                    .entity(entity)?;
            }
            PersistentWorkerNextTestsConnection(run_id) => {
                ctx.worker_next_tests_tasks
                    .insert(run_id.clone(), move |rx_stop| {
                        Self::start_persistent_next_tests_requests_task(
                            ctx.queues,
                            ctx.timeouts,
                            run_id,
                            stream,
                            rx_stop,
                            entity,
                        )
                    });
            }
        }

        Ok(())
    }

    async fn start_persistent_next_tests_requests_task(
        queues: SharedRuns,
        timeouts: RunTimeoutManager,
        run_id: RunId,
        mut conn: Box<dyn net_async::ServerStream>,
        mut rx_stop: oneshot::Receiver<()>,
        entity: Entity,
    ) -> OpaqueResult<()> {
        use net_protocol::work_server::NextTestRequest;

        loop {
            let NextTestRequest {} = tokio::select! {
                _ = &mut rx_stop => {
                    tracing::warn!(?run_id, entity_id=%entity.display_id(), entity_tag=%entity.tag,
                        "Stopping next-tests requests task before connection naturally expired");
                    return Ok(())
                }
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
            let next_work_result = { queues.next_work(&run_id) };

            match next_work_result {
                NextWorkResult::Present { bundle, status } => {
                    let response = NextTestResponse::Bundle(bundle);

                    net_protocol::async_write(&mut conn, &response)
                        .await
                        .located(here!())?;

                    if let PulledTestsStatus::PulledLastTest { last_test_timeout } = status {
                        // This was the last test, so start a timeout for the whole test run to
                        // complete - if it doesn't, we'll fire and timeout the run.
                        let timeout_spec = timeouts
                            .strategy()
                            .timeout_for_last_test_result(last_test_timeout);

                        tracing::info!(
                            ?run_id,
                            timeout=?timeout_spec.duration(),
                            entity_id=%entity.display_id(), entity_tag=%entity.tag,
                            "issued last test in the manifest to a worker"
                        );

                        timeouts.insert_run(run_id, timeout_spec).await;

                        return Ok(());
                    }

                    if status.reached_end_of_tests() {
                        // Exit, since the worker should not ask us for tests again.
                        return Ok(());
                    }
                }
                NextWorkResult::Pending => {
                    tracing::debug!(
                        ?run_id, entity_id=%entity.display_id(), entity_tag=%entity.tag,
                        "pending next tests in job queue"
                    );

                    net_protocol::async_write(&mut conn, &NextTestResponse::Pending)
                        .await
                        .located(here!())?;
                }
            }
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
    use std::{
        collections::HashMap,
        io,
        net::SocketAddr,
        sync::Arc,
        thread::{self, JoinHandle},
    };

    use super::{RunState, RunStateCache};
    use crate::{
        connections::ConnectedWorkers,
        invoke::DEFAULT_CLIENT_POLL_TIMEOUT,
        queue::{
            ActiveRunResponders, AddedManifest, AssignedRunLookup, BufferedResults, CancelReason,
            QueueServer, RunLive, SharedRuns, WorkScheduler,
        },
        supervisor_responder::SupervisorResponder,
        timeout::{RunTimeoutManager, RunTimeoutStrategy},
    };
    use abq_run_n_times::n_times;
    use abq_test_utils::{accept_handshake, assert_scoped_log, one_nonzero};
    use abq_utils::{
        auth::{
            build_strategies, Admin, AdminToken, ClientAuthStrategy, ServerAuthStrategy, User,
            UserToken,
        },
        net_opt::{ClientOptions, ServerOptions},
        net_protocol::{
            self,
            client::AckTestResults,
            entity::Entity,
            queue::{
                AckManifest, AssociatedTestResults, InvokeWork, InvokerTestData, NativeRunnerInfo,
                NegotiatorInfo, TestSpec,
            },
            runners::{NativeRunnerSpecification, TestCase, TestResult},
            work_server,
            workers::{RunId, WorkId, INIT_RUN_NUMBER},
        },
        shutdown::ShutdownManager,
        tls::{ClientTlsStrategy, ServerTlsStrategy},
    };
    use abq_with_protocol_version::with_protocol_version;
    use parking_lot as pl;
    use tokio::{
        io::AsyncWriteExt,
        sync::{Mutex, RwLock},
    };
    use tracing_test::traced_test;

    fn faux_invoke_work() -> InvokeWork {
        InvokeWork {
            run_id: RunId::unique(),
            batch_size_hint: one_nonzero(),
            test_results_timeout: DEFAULT_CLIENT_POLL_TIMEOUT,
            max_run_attempt: INIT_RUN_NUMBER,
        }
    }

    fn build_random_strategies() -> (
        ServerAuthStrategy,
        ClientAuthStrategy<User>,
        ClientAuthStrategy<Admin>,
    ) {
        build_strategies(UserToken::new_random(), AdminToken::new_random())
    }

    fn create_queue_server(
        server_auth: ServerAuthStrategy,
    ) -> (JoinHandle<()>, ShutdownManager, SocketAddr) {
        let server_opts = ServerOptions::new(server_auth, ServerTlsStrategy::no_tls());
        let server = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = server.local_addr().unwrap();

        let (shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();

        let queue_server = QueueServer {
            queues: Default::default(),
            public_negotiator_addr: "0.0.0.0:0".parse().unwrap(),
        };
        let server_thread = thread::spawn(|| {
            queue_server
                .start_on(
                    server,
                    RunTimeoutManager::new(RunTimeoutStrategy::default()),
                    shutdown_rx,
                    ConnectedWorkers::default(),
                )
                .unwrap();
        });

        (server_thread, shutdown_tx, server_addr)
    }

    #[test]
    #[traced_test]
    fn bad_message_doesnt_take_down_server() {
        let server = QueueServer::new(Default::default(), "0.0.0.0:0".parse().unwrap());

        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let listener = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();

        let (mut shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();
        let server_thread = thread::spawn(move || {
            server
                .start_on(
                    listener,
                    RunTimeoutManager::new(RunTimeoutStrategy::default()),
                    shutdown_rx,
                    ConnectedWorkers::default(),
                )
                .unwrap();
        });

        let client = client_opts.build().unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(&mut conn, "bad message").unwrap();
        let _fail: io::Result<u64> = net_protocol::read(&mut conn);

        shutdown_tx.shutdown_immediately().unwrap();
        server_thread.join().unwrap();

        assert_scoped_log("abq_queue::queue", "error handling connection");
    }

    #[tokio::test]
    async fn invoker_reconnection_succeeds() {
        let run_id = RunId::unique();
        let active_runs = ActiveRunResponders::default();

        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        // Set up an initial connection for streaming test results targeting the given run ID
        let fake_server = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let fake_server_addr = fake_server.local_addr().unwrap();

        let client = client_opts.build_async().unwrap();

        {
            // Register the initial connection
            let (client_res, server_res) = futures::join!(
                client.connect(fake_server_addr),
                accept_handshake(&*fake_server)
            );
            let (client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());

            let mut active_runs = active_runs.write().await;
            active_runs.insert(
                run_id.clone(),
                Mutex::new(SupervisorResponder::DirectStream {
                    stream: server_conn,
                    buffer: BufferedResults::new(5),
                }),
            );
            // Drop the connection
            drop(client_conn);
        };

        // Attempt to reconnect
        let (client_res, server_res) = futures::join!(
            client.connect(fake_server_addr),
            accept_handshake(&*fake_server)
        );
        let (client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());
        let new_conn_addr = client_conn.local_addr().unwrap();

        let reconnection_result = QueueServer::handle_invoker_reconnection(
            Arc::clone(&active_runs),
            Entity::supervisor(),
            server_conn,
            run_id.clone(),
        )
        .await;

        // Validate that the reconnection was granted
        assert!(reconnection_result.is_ok());
        let active_conn_addr = {
            let mut active_runs = active_runs.write().await;
            let responder = active_runs.remove(&run_id).unwrap().into_inner();
            match responder {
                SupervisorResponder::DirectStream { stream, .. } => stream.peer_addr().unwrap(),
                SupervisorResponder::Disconnected { .. } => unreachable!(),
            }
        };
        assert_eq!(active_conn_addr, new_conn_addr);
    }

    #[tokio::test]
    async fn invoker_reconnection_fails_never_invoked() {
        let run_id = RunId::unique();
        let active_runs = ActiveRunResponders::default();

        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let fake_server = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let fake_server_addr = fake_server.local_addr().unwrap();

        let client = client_opts.build_async().unwrap();

        let (client_res, server_res) = futures::join!(
            client.connect(fake_server_addr),
            accept_handshake(&*fake_server)
        );
        let (_client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());

        let reconnection_result = QueueServer::handle_invoker_reconnection(
            Arc::clone(&active_runs),
            Entity::supervisor(),
            server_conn,
            run_id.clone(),
        )
        .await;

        // Validate that the reconnection was granted
        assert!(reconnection_result.is_err());
        let err = reconnection_result.unwrap_err();
        assert!(err
            .error
            .to_string()
            .contains("never used to invoke work on the queue"));
    }

    #[tokio::test]
    async fn client_disconnect_then_connect_gets_buffered_results() {
        let run_id = RunId::unique();
        let active_runs = ActiveRunResponders::default();
        let run_queues = SharedRuns::default();
        let run_state = RunStateCache::default();
        let worker_results_tasks = ConnectedWorkers::default();
        let worker_next_tests_tasks = ConnectedWorkers::default();

        // Pretend we have infinite work so the queue always streams back results to the client.
        run_state
            .lock()
            .insert(run_id.clone(), super::ActiveRunState::new(usize::MAX));

        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let fake_server = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let fake_server_addr = fake_server.local_addr().unwrap();

        let client = client_opts.build_async().unwrap();

        {
            // Register the initial connection
            let (client_res, server_res) = futures::join!(
                client.connect(fake_server_addr),
                accept_handshake(&*fake_server)
            );
            let (client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());

            let mut active_runs = active_runs.write().await;
            let _responder = active_runs.insert(
                run_id.clone(),
                Mutex::new(SupervisorResponder::DirectStream {
                    stream: server_conn,
                    buffer: BufferedResults::new(0),
                }),
            );

            // Drop the client connection
            drop(client_conn);
        };

        let buffered_work_id = WorkId::new();
        {
            // Send a test result, will force the responder to go into disconnected mode
            let result = AssociatedTestResults::fake(buffered_work_id, vec![TestResult::fake()]);
            QueueServer::handle_worker_results(
                run_queues.clone(),
                Arc::clone(&active_runs),
                Arc::clone(&run_state),
                worker_results_tasks.clone(),
                worker_next_tests_tasks.clone(),
                Entity::runner(0, 1),
                run_id.clone(),
                vec![result],
            )
            .await
            .unwrap();

            // Check the internal state of the responder - it should be in disconnected mode.
            let responders = active_runs.write().await;
            let responder = responders.get(&run_id).unwrap().lock().await;
            match &*responder {
                SupervisorResponder::Disconnected { results, .. } => {
                    assert_eq!(results.len(), 1)
                }
                _ => unreachable!(),
            }
        }

        // Reconnect back to the queue
        let (client_res, server_res) = futures::join!(
            client.connect(fake_server_addr),
            accept_handshake(&*fake_server)
        );
        let (mut client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());

        {
            let reconnection_future = QueueServer::handle_invoker_reconnection(
                Arc::clone(&active_runs),
                Entity::runner(0, 1),
                server_conn,
                run_id.clone(),
            );

            let read_results_future = async {
                let AssociatedTestResults { work_id, .. } =
                    match net_protocol::async_read(&mut client_conn).await.unwrap() {
                        InvokerTestData::Results(mut results) => results.pop().unwrap(),
                        _ => panic!(),
                    };

                net_protocol::async_write(&mut client_conn, &AckTestResults {})
                    .await
                    .unwrap();

                assert_eq!(work_id, buffered_work_id);
            };

            let (reconnection_result, ()) = tokio::join!(reconnection_future, read_results_future);
            assert!(reconnection_result.is_ok());
        }

        {
            // Make sure that new test results also get send to the new client connectiion.
            let second_work_id = WorkId::new();
            let associated_result =
                AssociatedTestResults::fake(second_work_id, vec![TestResult::fake()]);
            let worker_result_future = QueueServer::handle_worker_results(
                run_queues,
                Arc::clone(&active_runs),
                run_state,
                worker_results_tasks.clone(),
                worker_next_tests_tasks.clone(),
                Entity::runner(0, 1),
                run_id,
                vec![associated_result],
            );

            let read_results_future = async {
                let AssociatedTestResults { work_id, .. } =
                    match net_protocol::async_read(&mut client_conn).await.unwrap() {
                        InvokerTestData::Results(mut results) => results.pop().unwrap(),
                        _ => panic!(),
                    };

                net_protocol::async_write(&mut client_conn, &AckTestResults {})
                    .await
                    .unwrap();

                assert_eq!(work_id, second_work_id);
            };

            let (worker_result_end, ()) = tokio::join!(worker_result_future, read_results_future);
            assert!(worker_result_end.is_ok());
        }
    }

    #[test]
    fn queue_server_healthcheck() {
        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let server = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = server.local_addr().unwrap();

        let (mut server_shutdown_tx, server_shutdown_rx) = ShutdownManager::new_pair();

        let queue_server = QueueServer {
            queues: Default::default(),
            public_negotiator_addr: "0.0.0.0:0".parse().unwrap(),
        };
        let queue_handle = thread::spawn(|| {
            queue_server
                .start_on(
                    server,
                    RunTimeoutManager::new(RunTimeoutStrategy::default()),
                    server_shutdown_rx,
                    ConnectedWorkers::default(),
                )
                .unwrap();
        });

        let client = client_opts.build().unwrap();
        let mut conn = client.connect(server_addr).unwrap();

        net_protocol::write(
            &mut conn,
            net_protocol::queue::Request {
                entity: Entity::local_client(),
                message: net_protocol::queue::Message::HealthCheck,
            },
        )
        .unwrap();
        let health_msg: net_protocol::health::Health = net_protocol::read(&mut conn).unwrap();

        assert_eq!(health_msg, net_protocol::health::healthy());

        server_shutdown_tx.shutdown_immediately().unwrap();
        queue_handle.join().unwrap();
    }

    #[test]
    fn work_server_healthcheck() {
        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let server = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = server.local_addr().unwrap();

        let (mut server_shutdown_tx, server_shutdown_rx) = ShutdownManager::new_pair();

        let work_scheduler = WorkScheduler {
            queues: Default::default(),
        };
        let work_server_handle = thread::spawn(|| {
            work_scheduler
                .start_on(
                    server,
                    RunTimeoutManager::new(RunTimeoutStrategy::default()),
                    server_shutdown_rx,
                    ConnectedWorkers::default(),
                )
                .unwrap();
        });

        let client = client_opts.build().unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        let request = work_server::Request {
            entity: Entity::local_client(),
            message: work_server::Message::HealthCheck,
        };
        net_protocol::write(&mut conn, &request).unwrap();
        let health_msg: net_protocol::health::Health = net_protocol::read(&mut conn).unwrap();

        assert_eq!(health_msg, net_protocol::health::healthy());

        server_shutdown_tx.shutdown_immediately().unwrap();
        work_server_handle.join().unwrap();
    }

    #[test]
    #[traced_test]
    fn bad_message_doesnt_take_down_work_scheduling_server() {
        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let server = WorkScheduler {
            queues: Default::default(),
        };

        let listener = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (mut shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();
        let server_thread = thread::spawn(move || {
            server
                .start_on(
                    listener,
                    RunTimeoutManager::new(RunTimeoutStrategy::default()),
                    shutdown_rx,
                    ConnectedWorkers::default(),
                )
                .unwrap();
        });

        let client = client_opts.build().unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(&mut conn, "bad message").unwrap();
        let _fail: io::Result<u64> = net_protocol::read(&mut conn);

        shutdown_tx.shutdown_immediately().unwrap();
        server_thread.join().unwrap();

        assert_scoped_log("abq_queue::queue", "error handling connection");
    }

    #[test]
    #[traced_test]
    fn connecting_to_queue_server_with_auth_okay() {
        let negotiator_addr = "0.0.0.0:0".parse().unwrap();
        let server = QueueServer::new(Default::default(), negotiator_addr);

        let (server_auth, client_auth, _) =
            build_strategies(UserToken::new_random(), AdminToken::new_random());
        let server_opts = ServerOptions::new(server_auth, ServerTlsStrategy::no_tls());
        let client_opts = ClientOptions::new(client_auth, ClientTlsStrategy::no_tls());

        let listener = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (mut shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();
        let server_thread = thread::spawn(move || {
            server
                .start_on(
                    listener,
                    RunTimeoutManager::new(RunTimeoutStrategy::default()),
                    shutdown_rx,
                    ConnectedWorkers::default(),
                )
                .unwrap();
        });

        let client = client_opts.build().unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(
            &mut conn,
            net_protocol::queue::Request {
                entity: Entity::local_client(),
                message: net_protocol::queue::Message::NegotiatorInfo {
                    run_id: RunId::unique(),
                    deprecations: Default::default(),
                },
            },
        )
        .unwrap();

        let NegotiatorInfo {
            negotiator_address: recv_negotiator_addr,
            version,
        } = net_protocol::read(&mut conn).unwrap();
        assert_eq!(negotiator_addr, recv_negotiator_addr);
        assert_eq!(version, abq_utils::VERSION);

        shutdown_tx.shutdown_immediately().unwrap();
        server_thread.join().unwrap();
    }

    #[test]
    #[traced_test]
    fn connecting_to_queue_server_with_no_auth_fails() {
        let negotiator_addr = "0.0.0.0:0".parse().unwrap();
        let server = QueueServer::new(Default::default(), negotiator_addr);

        let (server_auth, _, _) = build_random_strategies();

        let server_opts = ServerOptions::new(server_auth, ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let listener = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (mut shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();
        let server_thread = thread::spawn(move || {
            server
                .start_on(
                    listener,
                    RunTimeoutManager::new(RunTimeoutStrategy::default()),
                    shutdown_rx,
                    ConnectedWorkers::default(),
                )
                .unwrap();
        });

        let client = client_opts.build().unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(
            &mut conn,
            net_protocol::queue::Request {
                entity: Entity::local_client(),
                message: net_protocol::queue::Message::NegotiatorInfo {
                    run_id: RunId::unique(),
                    deprecations: Default::default(),
                },
            },
        )
        .unwrap();
        let fail: io::Result<NegotiatorInfo> = net_protocol::read(&mut conn);
        assert!(fail.is_err());

        shutdown_tx.shutdown_immediately().unwrap();
        server_thread.join().unwrap();

        assert_scoped_log("abq_queue::queue", "error handling connection");
    }

    #[test]
    #[traced_test]
    fn connecting_to_work_server_with_auth_okay() {
        let server = WorkScheduler {
            queues: Default::default(),
        };

        let (server_auth, client_auth, _) = build_random_strategies();
        let server_opts = ServerOptions::new(server_auth, ServerTlsStrategy::no_tls());
        let client_opts = ClientOptions::new(client_auth, ClientTlsStrategy::no_tls());

        let listener = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (mut shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();
        let server_thread = thread::spawn(move || {
            server
                .start_on(
                    listener,
                    RunTimeoutManager::new(RunTimeoutStrategy::default()),
                    shutdown_rx,
                    ConnectedWorkers::default(),
                )
                .unwrap();
        });

        let client = client_opts.build().unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        let request = work_server::Request {
            entity: Entity::local_client(),
            message: work_server::Message::HealthCheck,
        };
        net_protocol::write(&mut conn, &request).unwrap();

        let health_msg: net_protocol::health::Health = net_protocol::read(&mut conn).unwrap();

        assert_eq!(health_msg, net_protocol::health::healthy());

        shutdown_tx.shutdown_immediately().unwrap();
        server_thread.join().unwrap();
    }

    #[test]
    #[traced_test]
    fn connecting_to_work_server_with_no_auth_fails() {
        let server = WorkScheduler {
            queues: Default::default(),
        };

        let (server_auth, _, _) = build_random_strategies();
        let server_opts = ServerOptions::new(server_auth, ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let listener = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (mut shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();
        let server_thread = thread::spawn(move || {
            server
                .start_on(
                    listener,
                    RunTimeoutManager::new(RunTimeoutStrategy::default()),
                    shutdown_rx,
                    ConnectedWorkers::default(),
                )
                .unwrap();
        });

        let client = client_opts.build().unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        let request = work_server::Request {
            entity: Entity::local_client(),
            message: work_server::Message::HealthCheck,
        };
        net_protocol::write(&mut conn, &request).unwrap();

        shutdown_tx.shutdown_immediately().unwrap();
        server_thread.join().unwrap();

        assert_scoped_log("abq_queue::queue", "error handling connection");
    }

    #[test]
    fn no_active_runs_when_non_enqueued() {
        let queues = SharedRuns::default();

        assert_eq!(queues.estimate_num_active_runs(), 0);
    }

    #[test]
    #[with_protocol_version]
    fn active_runs_when_some_waiting_on_workers() {
        let queues = SharedRuns::default();

        queues
            .create_queue(
                RunId::unique(),
                one_nonzero(),
                INIT_RUN_NUMBER,
                DEFAULT_CLIENT_POLL_TIMEOUT,
            )
            .unwrap();

        assert_eq!(queues.estimate_num_active_runs(), 1);
    }

    #[test]
    #[with_protocol_version]
    fn active_runs_when_some_waiting_on_manifest() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        queues
            .create_queue(
                run_id.clone(),
                one_nonzero(),
                INIT_RUN_NUMBER,
                DEFAULT_CLIENT_POLL_TIMEOUT,
            )
            .unwrap();

        queues.find_run_for_worker(&run_id, Entity::local_client());

        assert_eq!(queues.estimate_num_active_runs(), 1);
    }

    #[test]
    #[with_protocol_version]
    fn active_runs_when_running() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        queues
            .create_queue(
                run_id.clone(),
                one_nonzero(),
                INIT_RUN_NUMBER,
                DEFAULT_CLIENT_POLL_TIMEOUT,
            )
            .unwrap();

        queues.find_run_for_worker(&run_id, Entity::local_client());
        let added = queues.add_manifest(&run_id, vec![], Default::default());
        assert!(matches!(added, AddedManifest::Added { .. }));

        assert_eq!(queues.estimate_num_active_runs(), 1);
    }

    #[test]
    #[with_protocol_version]
    fn active_runs_when_all_done() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        queues
            .create_queue(
                run_id.clone(),
                one_nonzero(),
                INIT_RUN_NUMBER,
                DEFAULT_CLIENT_POLL_TIMEOUT,
            )
            .unwrap();

        queues.find_run_for_worker(&run_id, Entity::local_client());
        let added = queues.add_manifest(&run_id, vec![], Default::default());
        assert!(matches!(added, AddedManifest::Added { .. }));
        queues.mark_complete(&run_id);

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
            queues
                .create_queue(
                    run_id.clone(),
                    one_nonzero(),
                    INIT_RUN_NUMBER,
                    DEFAULT_CLIENT_POLL_TIMEOUT,
                )
                .unwrap();

            queues.find_run_for_worker(run_id, Entity::local_client());
        }

        for run_id in [&run_id1, &run_id2, &run_id4] {
            let added = queues.add_manifest(run_id, vec![], Default::default());
            assert!(matches!(added, AddedManifest::Added { .. }));
        }

        for run_id in [run_id1, run_id2] {
            queues.mark_complete(&run_id);
        }

        assert_eq!(queues.estimate_num_active_runs(), expected_active);
    }

    #[test]
    #[with_protocol_version]
    fn mark_cancellation_when_waiting_on_workers() {
        let queues = SharedRuns::default();
        let run_id = RunId::unique();

        queues
            .create_queue(
                run_id.clone(),
                one_nonzero(),
                INIT_RUN_NUMBER,
                DEFAULT_CLIENT_POLL_TIMEOUT,
            )
            .unwrap();

        assert_eq!(queues.estimate_num_active_runs(), 1);

        queues.mark_cancelled(&run_id, CancelReason::User);

        assert_eq!(queues.estimate_num_active_runs(), 0);
        assert!(matches!(
            queues.get_run_liveness(&run_id).unwrap(),
            RunLive::Cancelled
        ));
    }

    #[test]
    #[with_protocol_version]
    fn mark_cancellation_when_some_waiting_on_manifest() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        queues
            .create_queue(
                run_id.clone(),
                one_nonzero(),
                INIT_RUN_NUMBER,
                DEFAULT_CLIENT_POLL_TIMEOUT,
            )
            .unwrap();

        queues.find_run_for_worker(&run_id, Entity::local_client());

        assert_eq!(queues.estimate_num_active_runs(), 1);

        queues.mark_cancelled(&run_id, CancelReason::User);

        assert_eq!(queues.estimate_num_active_runs(), 0);
        assert!(matches!(
            queues.get_run_liveness(&run_id).unwrap(),
            RunLive::Cancelled
        ));
    }

    #[test]
    #[with_protocol_version]
    fn mark_cancellation_when_running() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        queues
            .create_queue(
                run_id.clone(),
                one_nonzero(),
                INIT_RUN_NUMBER,
                DEFAULT_CLIENT_POLL_TIMEOUT,
            )
            .unwrap();

        queues.find_run_for_worker(&run_id, Entity::local_client());
        let added = queues.add_manifest(&run_id, vec![], Default::default());
        assert!(matches!(added, AddedManifest::Added { .. }));

        assert_eq!(queues.estimate_num_active_runs(), 1);

        queues.mark_cancelled(&run_id, CancelReason::User);

        assert_eq!(queues.estimate_num_active_runs(), 0);
        assert!(matches!(
            queues.get_run_liveness(&run_id).unwrap(),
            RunLive::Cancelled
        ));
    }

    #[test]
    #[with_protocol_version]
    fn mark_worker_waiting_for_supervisor() {
        let queues = SharedRuns::default();

        let run_id = RunId::unique();

        let assigned_run = queues.find_run_for_worker(&run_id, Entity::local_client());
        assert!(matches!(assigned_run, AssignedRunLookup::NotFound));

        assert_eq!(queues.estimate_num_active_runs(), 0);
        assert!(queues.get_run_liveness(&run_id).is_none());
    }

    #[test]
    #[with_protocol_version]
    #[n_times(1_000)]
    fn mark_worker_waiting_for_supervisor_and_supervisor_connects_always_creates_run() {
        // Tests that if a worker connects, around the same time the supervisor connects to create
        // a test run queue, we always end up in the state where the run has been created, and not
        // in the waiting-for-supervisor state.

        let queues = SharedRuns::default();
        let run_id = RunId::unique();

        let create_queue_task = {
            let queues = queues.clone();
            let run_id = run_id.clone();
            move || {
                queues
                    .create_queue(
                        run_id,
                        one_nonzero(),
                        INIT_RUN_NUMBER,
                        DEFAULT_CLIENT_POLL_TIMEOUT,
                    )
                    .unwrap();
            }
        };

        let find_run_for_worker_task = {
            let queues = queues.clone();
            let run_id = run_id.clone();
            move || {
                queues.find_run_for_worker(&run_id, Entity::local_client());
            }
        };

        let (create_queue_handle, find_run_handle) = (
            thread::spawn(create_queue_task),
            thread::spawn(find_run_for_worker_task),
        );
        create_queue_handle.join().unwrap();
        find_run_handle.join().unwrap();

        assert_eq!(queues.estimate_num_active_runs(), 1);
        assert!(matches!(
            queues.get_run_liveness(&run_id),
            Some(RunLive::Active)
        ));
        // Either the supervisor should have set the final state and said we're waiting for the
        // first worker, or the worker connected after the queue creation, so we're waiting for the
        // manifest.
        assert!(matches!(
            queues.0.runs.read().get(&run_id).unwrap().lock().state,
            RunState::WaitingForFirstWorker { .. } | RunState::WaitingForManifest { .. }
        ));
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

        // Register the invoker connection
        let (client_res, server_res) = futures::join!(
            client.connect(fake_server_addr),
            accept_handshake(&*fake_server)
        );
        let (mut client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());

        // Build up our initial state - run is registered, no manifest yet
        let run_id = RunId::unique();
        let queues = {
            let queues = SharedRuns::default();
            queues
                .create_queue(
                    run_id.clone(),
                    one_nonzero(),
                    INIT_RUN_NUMBER,
                    DEFAULT_CLIENT_POLL_TIMEOUT,
                )
                .unwrap();
            queues.find_run_for_worker(&run_id, Entity::local_client());
            queues
        };
        let active_runs: ActiveRunResponders = {
            let mut map = HashMap::default();
            map.insert(
                run_id.clone(),
                Mutex::new(SupervisorResponder::new(server_conn, 1)),
            );
            Arc::new(RwLock::new(map))
        };
        let state_cache: RunStateCache = {
            let mut cache = HashMap::default();
            cache.insert(run_id.clone(), super::ActiveRunState::new(1));
            Arc::new(pl::Mutex::new(cache))
        };
        let worker_results_tasks = ConnectedWorkers::default();
        let worker_next_tests_tasks = ConnectedWorkers::default();

        let entity = Entity::local_client();

        // Cancel the run, make sure we exit smoothly
        {
            let cancellation_fut = QueueServer::handle_run_cancellation(
                queues.clone(),
                Arc::clone(&active_runs),
                Arc::clone(&state_cache),
                entity,
                run_id.clone(),
            );
            let client_exit_fut = client_conn.shutdown();

            let (cancellation_end, client_exit_end) =
                futures::join!(cancellation_fut, client_exit_fut);
            assert!(cancellation_end.is_ok());
            assert!(client_exit_end.is_ok());
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
                active_runs.clone(),
                state_cache.clone(),
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
            queues.get_run_liveness(&run_id).unwrap(),
            RunLive::Cancelled {}
        ));
        assert!(!active_runs.read().await.contains_key(&run_id));
        assert!(!state_cache.lock().contains_key(&run_id));
        assert!(!worker_results_tasks.has_tasks_for(&run_id));
        assert!(!worker_next_tests_tasks.has_tasks_for(&run_id));
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

        // Register the invoker connection
        let (client_res, server_res) = futures::join!(
            client.connect(fake_server_addr),
            accept_handshake(&*fake_server)
        );
        let (mut client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());

        // Build up our initial state - run is registered, no manifest yet
        let run_id = RunId::unique();
        let queues = {
            let queues = SharedRuns::default();
            queues
                .create_queue(
                    run_id.clone(),
                    one_nonzero(),
                    INIT_RUN_NUMBER,
                    DEFAULT_CLIENT_POLL_TIMEOUT,
                )
                .unwrap();
            queues.find_run_for_worker(&run_id, Entity::local_client());
            queues
        };
        let active_runs: ActiveRunResponders = {
            let mut map = HashMap::default();
            map.insert(
                run_id.clone(),
                Mutex::new(SupervisorResponder::new(server_conn, 1)),
            );
            Arc::new(RwLock::new(map))
        };
        let state_cache: RunStateCache = {
            let mut cache = HashMap::default();
            cache.insert(run_id.clone(), super::ActiveRunState::new(1));
            Arc::new(pl::Mutex::new(cache))
        };
        let worker_results_tasks = ConnectedWorkers::default();
        let worker_next_tests_tasks = ConnectedWorkers::default();

        let entity = Entity::local_client();

        // Cancel the run, make sure we exit smoothly
        let do_cancellation_fut = async {
            let cancellation_fut = QueueServer::handle_run_cancellation(
                queues.clone(),
                Arc::clone(&active_runs),
                Arc::clone(&state_cache),
                entity,
                run_id.clone(),
            );
            let client_exit_fut = client_conn.shutdown();

            let (cancellation_end, client_exit_end) =
                futures::future::join(cancellation_fut, client_exit_fut).await;
            assert!(cancellation_end.is_ok());
            assert!(client_exit_end.is_ok());
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
            let active_runs = active_runs.clone();
            let run_id = run_id.clone();
            let state_cache = state_cache.clone();

            async move {
                let handle_manifest_fut = QueueServer::handle_manifest_success(
                    queues,
                    active_runs,
                    state_cache,
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
            queues.get_run_liveness(&run_id).unwrap(),
            RunLive::Cancelled {}
        ));
        assert!(!active_runs.read().await.contains_key(&run_id));
        assert!(!state_cache.lock().contains_key(&run_id));
        assert!(!worker_results_tasks.has_tasks_for(&run_id));
        assert!(!worker_next_tests_tasks.has_tasks_for(&run_id));
    }

    #[tokio::test]
    #[n_times(1_000)]
    #[with_protocol_version]
    async fn receiving_cancellation_during_last_test_results_is_cancellation() {
        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let fake_server = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let fake_server_addr = fake_server.local_addr().unwrap();

        let client = client_opts.build_async().unwrap();

        // Register the initial connection
        let (client_res, server_res) = futures::join!(
            client.connect(fake_server_addr),
            accept_handshake(&*fake_server)
        );
        let (mut client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());

        let run_id = RunId::unique();
        let queues = {
            let queues = SharedRuns::default();
            queues
                .create_queue(
                    run_id.clone(),
                    one_nonzero(),
                    INIT_RUN_NUMBER,
                    DEFAULT_CLIENT_POLL_TIMEOUT,
                )
                .unwrap();
            queues.find_run_for_worker(&run_id, Entity::local_client());
            let added = queues.add_manifest(&run_id, vec![], Default::default());
            assert!(matches!(added, AddedManifest::Added { .. }));
            queues
        };
        let active_runs: ActiveRunResponders = {
            let mut map = HashMap::default();
            map.insert(
                run_id.clone(),
                Mutex::new(SupervisorResponder::new(server_conn, 1)),
            );
            Arc::new(RwLock::new(map))
        };
        let state_cache: RunStateCache = {
            let mut cache = HashMap::default();
            cache.insert(run_id.clone(), super::ActiveRunState::new(1));
            Arc::new(pl::Mutex::new(cache))
        };
        let worker_results_tasks = ConnectedWorkers::default();
        let worker_next_tests_tasks = ConnectedWorkers::default();

        let entity = Entity::local_client();

        let result = AssociatedTestResults::fake(WorkId::new(), vec![TestResult::fake()]);
        let send_last_result_fut = QueueServer::handle_worker_results(
            queues.clone(),
            Arc::clone(&active_runs),
            Arc::clone(&state_cache),
            worker_results_tasks.clone(),
            worker_next_tests_tasks.clone(),
            entity,
            run_id.clone(),
            vec![result],
        );
        let cancellation_fut = QueueServer::handle_run_cancellation(
            queues.clone(),
            Arc::clone(&active_runs),
            Arc::clone(&state_cache),
            entity,
            run_id.clone(),
        );
        let client_exit_fut = client_conn.shutdown();

        let (send_last_result_end, cancellation_end, client_exit_end) =
            futures::join!(send_last_result_fut, cancellation_fut, client_exit_fut);

        assert!(send_last_result_end.is_ok());
        assert!(cancellation_end.is_ok());
        assert!(client_exit_end.is_ok());

        assert!(matches!(
            queues.get_run_liveness(&run_id).unwrap(),
            RunLive::Cancelled {}
        ));
        assert!(!active_runs.read().await.contains_key(&run_id));
        assert!(!state_cache.lock().contains_key(&run_id));
        assert!(!worker_results_tasks.has_tasks_for(&run_id));
        assert!(!worker_next_tests_tasks.has_tasks_for(&run_id));
    }

    #[test]
    #[with_protocol_version]
    fn accept_retirement_request() {
        use net_protocol::queue;

        let (server_auth, client_auth, admin_auth) = build_random_strategies();

        let (queue_thread, mut queue_shutdown, server_addr) = create_queue_server(server_auth);

        {
            let admin = ClientOptions::new(admin_auth, ClientTlsStrategy::no_tls())
                .build()
                .unwrap();
            let mut conn = admin.connect(server_addr).unwrap();

            net_protocol::write(
                &mut conn,
                queue::Request {
                    entity: Entity::local_client(),
                    message: net_protocol::queue::Message::Retire,
                },
            )
            .unwrap();
            let ack = net_protocol::read(&mut conn).unwrap();

            assert!(matches!(ack, queue::AckRetirement {}));
            assert!(queue_shutdown.is_retired());
        }

        // Retirement should now reject new test run connections.
        {
            let client = ClientOptions::new(client_auth, ClientTlsStrategy::no_tls())
                .build()
                .unwrap();
            let mut conn = client.connect(server_addr).unwrap();

            net_protocol::write(
                &mut conn,
                queue::Request {
                    entity: Entity::local_client(),
                    message: queue::Message::InvokeWork(faux_invoke_work()),
                },
            )
            .unwrap();

            let result: Result<queue::InvokeWorkResponse, _> = net_protocol::read(&mut conn);

            assert!(result.is_err());
        }

        queue_shutdown.shutdown_immediately().unwrap();
        queue_thread.join().unwrap();
    }

    #[test]
    fn reject_retirement_request_from_non_admin() {
        use net_protocol::queue;

        let (server_auth, client_auth, _admin_auth) = build_random_strategies();

        let (queue_thread, mut queue_shutdown, server_addr) = create_queue_server(server_auth);

        let non_admin = ClientOptions::new(client_auth, ClientTlsStrategy::no_tls())
            .build()
            .unwrap();
        let mut conn = non_admin.connect(server_addr).unwrap();

        net_protocol::write(
            &mut conn,
            queue::Request {
                entity: Entity::local_client(),
                message: net_protocol::queue::Message::Retire,
            },
        )
        .unwrap();

        let result: Result<queue::AckRetirement, _> = net_protocol::read(&mut conn);

        assert!(result.is_err());
        assert!(!queue_shutdown.is_retired());

        queue_shutdown.shutdown_immediately().unwrap();
        queue_thread.join().unwrap();
    }
}

#[cfg(test)]
mod test_pull_work {
    use std::time::Duration;

    use abq_test_utils::one_nonzero;
    use abq_utils::net_protocol::workers::{NextWork, RunId, WorkerTest};
    use abq_with_protocol_version::with_protocol_version;

    use crate::{
        attempts::AttemptsCounter,
        queue::{
            fake_test_spec, JobQueue, NextWorkResult, PulledTestsStatus, RunLive, RunState,
            SharedRuns,
        },
    };

    #[test]
    #[with_protocol_version]
    fn pull_last_tests_for_non_final_test_run_attempt_shows_tests_pending() {
        let mut queue = JobQueue::default();
        queue.add_batch_work([WorkerTest {
            spec: fake_test_spec(proto),
            run_number: 1,
        }]);

        let has_work = RunState::HasWork {
            queue,
            init_metadata: Default::default(),
            batch_size_hint: one_nonzero(),
            last_test_timeout: Duration::ZERO,
            attempts_counter: AttemptsCounter::new(4),
        };

        let run_id = RunId::unique();
        let queues = SharedRuns::default();
        queues.set_state(run_id.clone(), has_work);

        // First worker pulls to the end of the queue, but there should be additional attempts
        // following it.
        let next_work = queues.next_work(&run_id);

        assert!(
            matches!(next_work, NextWorkResult::Present { bundle, status }
            if bundle.work.len() == 1 && matches!(status, PulledTestsStatus::MoreTestsRemaining)
            )
        );

        assert!(matches!(
            queues.get_run_liveness(&run_id),
            Some(RunLive::Active)
        ));

        // Now, if another worker pulls work it should get nothing, but be told the remaining work
        // is pending.
        let next_work = queues.next_work(&run_id);

        assert!(matches!(next_work, NextWorkResult::Pending));

        assert!(matches!(
            queues.get_run_liveness(&run_id),
            Some(RunLive::Active)
        ));
    }

    #[test]
    #[with_protocol_version]
    fn pull_last_tests_for_final_test_run_attempt_shows_tests_complete() {
        // Set up the queue so only one test is pulled, and there are a total of two attempts.
        let mut queue = JobQueue::default();
        queue.add_batch_work([
            WorkerTest {
                spec: fake_test_spec(proto),
                run_number: 1,
            },
            WorkerTest {
                spec: fake_test_spec(proto),
                run_number: 2,
            },
        ]);

        let batch_size_hint = one_nonzero();
        let attempts_counter = AttemptsCounter::new(2);

        let has_work = RunState::HasWork {
            queue,
            init_metadata: Default::default(),
            batch_size_hint,
            last_test_timeout: Duration::ZERO,
            attempts_counter,
        };

        let run_id = RunId::unique();
        let queues = SharedRuns::default();
        queues.set_state(run_id.clone(), has_work);

        // First pull. There should be additional tests following it.
        {
            let next_work = queues.next_work(&run_id);

            assert!(
                matches!(next_work, NextWorkResult::Present { bundle, status }
                    if bundle.work.len() == 1 && matches!(status, PulledTestsStatus::MoreTestsRemaining)
                )
            );

            assert!(matches!(
                queues.get_run_liveness(&run_id),
                Some(RunLive::Active)
            ));
        }

        // Second pull. Now we reach the end of all tests and all test run attempts.
        {
            let next_work = queues.next_work(&run_id);

            assert!(
                matches!(next_work, NextWorkResult::Present { bundle, status }
                    if bundle.work.len() == 2 // 1 - test, 1 - end of work
                    && matches!(bundle.work.last(), Some(NextWork::EndOfWork))
                    && matches!(status, PulledTestsStatus::PulledLastTest { .. })
                )
            );

            assert!(matches!(
                queues.get_run_liveness(&run_id),
                Some(RunLive::Active)
            ));
        }

        // Now, if another worker pulls work it should get nothing other than an end-of-test marker,
        // and be told we're all done.
        {
            let next_work = queues.next_work(&run_id);

            assert!(
                matches!(next_work, NextWorkResult::Present { bundle, status }
                    if bundle.work.len() == 1
                    && matches!(bundle.work.last(), Some(NextWork::EndOfWork))
                    && matches!(status, PulledTestsStatus::QueueWasEmpty)
                )
            );

            assert!(matches!(
                queues.get_run_liveness(&run_id),
                Some(RunLive::Active)
            ));
        }
    }
}
