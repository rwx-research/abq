use std::{net::SocketAddr, time::Duration};

use abq_generic_test_runner::TestsFetcher;
use abq_utils::{
    log_assert,
    net_async::ConfiguredClient,
    net_protocol::{
        entity::Entity,
        error::FetchTestsError,
        workers::{Eow, NextWorkBundle, RunId},
    },
};
use async_trait::async_trait;
use tracing::instrument;

mod out_of_process_retry_manifest_fetcher;
mod persistent_test_fetcher;
mod retries;

use out_of_process_retry_manifest_fetcher::OutOfProcessRetryManifestFetcher;
use persistent_test_fetcher::PersistedTestsFetcher;
pub use retries::ResultsTracker;
use retries::RetryTracker;

/// The provider of tests to an ABQ runner on a worker node.
///
/// Tests are provided via one of three sources:
///   - Online, from a fresh test schedule provided by a remote queue (FRESH)
///   - Offline, from a list of tests to retry in-band (IN_BAND_RETRY)
///   - Offline, from a previous run's test schedule provided by a remote queue (NEW_PROCESS_RETRY)
///
/// FRESH: this is the source of tests for all new ABQ runs; slices of the manifest to execute are
/// streamed to a worker.
///
/// IN_BAND_RETRY: if retries are configured for a run, a worker collects from the FRESH manifest
/// tests that should be retried into a retry-manifest. Once the FRESH manifest is exhausted, this
/// is the source of new tests.
///
/// NEW_PROCESS_RETRY: takes the place of FRESH if this is a re-run of a previously-completed
/// execution of a test suite by a worker.
pub struct Fetcher {
    initial_source: Option<InitialSource>,
    retry_source: RetryTracker,
    pending_retry_manifest_wait_time: Duration,
}

const DEFAULT_PENDING_RETRY_MANIFEST_WAIT_TIME: Duration = Duration::from_millis(100);

/// How the initial manifest from the queue should be sourced.
pub enum SourcingStrategy {
    /// This runner is new to the given test; use the FRESH strategy.
    Fresh,
    /// This runner is retrying a given test run; use NEW_PROCESS_RETRY strategy.
    Retry,
}

impl Fetcher {
    pub fn new(
        sourcing_strategy: SourcingStrategy,
        entity: Entity,
        work_server_addr: SocketAddr,
        client: Box<dyn ConfiguredClient>,
        run_id: RunId,
        max_run_number: u32,
    ) -> (Self, ResultsTracker) {
        let initial_source = InitialSource::from_strategy(
            sourcing_strategy,
            entity,
            work_server_addr,
            client,
            run_id,
        );

        Self::new_help(
            initial_source,
            max_run_number,
            DEFAULT_PENDING_RETRY_MANIFEST_WAIT_TIME,
        )
    }

    fn new_help(
        initial_source: InitialSource,
        max_run_number: u32,
        pending_retry_manifest_wait_time: Duration,
    ) -> (Self, ResultsTracker) {
        let (retry_source, results_tracker) = retries::build_tracking_pair(max_run_number);
        let me = Self {
            initial_source: Some(initial_source),
            retry_source,
            pending_retry_manifest_wait_time,
        };
        (me, results_tracker)
    }
}

/// The initial source of tests; this is either FRESH or NEW_PROCESS_RETRY.
enum InitialSource {
    Fresh(PersistedTestsFetcher),
    Retry(OutOfProcessRetryManifestFetcher),
}

impl InitialSource {
    fn from_strategy(
        strategy: SourcingStrategy,
        entity: Entity,
        work_server_addr: SocketAddr,
        client: Box<dyn ConfiguredClient>,
        run_id: RunId,
    ) -> Self {
        match strategy {
            SourcingStrategy::Fresh => Self::Fresh(persistent_test_fetcher::start(
                entity,
                work_server_addr,
                client,
                run_id,
            )),
            SourcingStrategy::Retry => Self::Retry(OutOfProcessRetryManifestFetcher::new(
                entity,
                work_server_addr,
                client,
                run_id,
            )),
        }
    }
}

impl Fetcher {
    async fn fetch_next_tests(&mut self) -> Result<NextWorkBundle, FetchTestsError> {
        // Test fetching works in two phases:
        //
        //   1. Fetch the schedule of the manifest to be run by the worker from the queue, either
        //      online (FRESH) or offline (NEW_PROCESS_RETRY).
        //      This schedule is used to hydrate the retry manifest tracker as it comes in.
        //   2. After the full manifest has been pulled, defer to the manifest tracker to supply
        //      all tests that should be re-run.
        //      This phase consists only of retries, and supplies tests to be run only once all
        //      test results in the last suite run attempt have been accounted for.
        //      One the retry manifest issues [NextWork::EndOfWork], there are no more tests to be
        //      run.
        loop {
            let initial_source = std::mem::take(&mut self.initial_source);

            match initial_source {
                Some(source) => {
                    let tests = match source {
                        InitialSource::Fresh(mut s) => {
                            let tests = s.get_next_tests().await;
                            // Put the source back, we may need it to fetch incremental tests again.
                            self.initial_source = Some(InitialSource::Fresh(s));
                            tests
                        }
                        InitialSource::Retry(s) => s.get_next_tests().await?,
                    };

                    let hydration_status = self
                        .retry_source
                        .hydrate_ordered_manifest_slice(tests.work.clone(), tests.eow);

                    match hydration_status {
                        retries::HydrationStatus::StillHydrating => {
                            return Ok(tests);
                        }
                        retries::HydrationStatus::EmptyManifest => {
                            log_assert!(tests.eow.0, "received empty manifest, but not EOW");
                            return Ok(tests);
                        }
                        retries::HydrationStatus::EndOfManifest => {
                            log_assert!(
                                tests.eow.0,
                                "reached hydration end of manifest status, but not EOW"
                            );

                            self.initial_source = None;

                            if tests.work.is_empty() {
                                // This is a non-empty manifest but there is no work in this batch;
                                // that means all work must have already been handed out.
                                // Move into waiting for the retry manifest, which will resolve
                                // once we've handled all test results.
                                continue;
                            } else {
                                // Set EndOfWork false so that the worker returns for the retry manifest,
                                // rather than finishing at this point.
                                let mut tests = tests;
                                tests.eow = Eow(false);
                                return Ok(tests);
                            }
                        }
                    }
                }
                None => {
                    loop {
                        match self.retry_source.try_assemble_retry_manifest() {
                            Some(bundle) => {
                                return Ok(bundle);
                            }
                            // The retry manifest is not yet ready; we must be waiting for more results
                            // to come in, before we know the status of the retry manifest.
                            //
                            // TODO do something smarter than spinning here. For example we can pass
                            // down a notifier of when we should be awoken.
                            None => tokio::time::sleep(self.pending_retry_manifest_wait_time).await,
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl TestsFetcher for Fetcher {
    #[instrument(level = "trace", skip(self))]
    async fn get_next_tests(&mut self) -> Result<NextWorkBundle, FetchTestsError> {
        self.fetch_next_tests().await
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use abq_generic_test_runner::TestsFetcher;
    use abq_test_utils::{
        spec, test, wid, with_focus, AssociatedTestResultsBuilder, TestResultBuilder,
    };
    use abq_utils::net_protocol::workers::{Eow, NextWorkBundle, WorkerTest, INIT_RUN_NUMBER};

    use crate::test_fetching::retries::test::{FAILURE, SUCCESS};

    use super::{
        out_of_process_retry_manifest_fetcher, persistent_test_fetcher, Fetcher, InitialSource,
    };

    #[tokio::test]
    async fn empty_manifest_fresh() {
        use persistent_test_fetcher::test::{
            scaffold_server, server_establish, server_send_bundle,
        };

        let (server, queue_fetcher) = scaffold_server(1).await;
        let (mut fetcher, _results) = Fetcher::new_help(
            InitialSource::Fresh(queue_fetcher),
            INIT_RUN_NUMBER,
            Duration::MAX,
        );

        let server_task = async move {
            let mut conn = server_establish(&*server).await;
            server_send_bundle(&mut conn, [], Eow(true)).await;
        };

        let ((), bundle) = tokio::join!(server_task, fetcher.get_next_tests());

        let NextWorkBundle { work, eow } = bundle.unwrap();

        assert!(work.is_empty());
        assert!(eow.0);
    }

    #[tokio::test]
    async fn empty_manifest_retry() {
        use out_of_process_retry_manifest_fetcher::test::{
            scaffold_server, server_establish, server_send_bundle,
        };

        let (server, queue_fetcher) = scaffold_server().await;
        let (mut fetcher, _results) = Fetcher::new_help(
            InitialSource::Retry(queue_fetcher),
            INIT_RUN_NUMBER,
            Duration::MAX,
        );

        let server_task = async move {
            let mut conn = server_establish(&*server).await;
            server_send_bundle(&mut conn, [], Eow(true)).await;
        };

        let ((), bundle) = tokio::join!(server_task, fetcher.get_next_tests());

        let NextWorkBundle { work, eow } = bundle.unwrap();

        assert!(work.is_empty());
        assert!(eow.0);
    }

    #[tokio::test]
    async fn fetch_incremental_from_queue_fresh() {
        use persistent_test_fetcher::test::{
            scaffold_server, server_establish, server_send_bundle,
        };

        let (server, queue_fetcher) = scaffold_server(1).await;
        let (mut fetcher, _results) = Fetcher::new_help(
            InitialSource::Fresh(queue_fetcher),
            INIT_RUN_NUMBER,
            Duration::MAX,
        );

        let server_task = async move {
            let mut conn = server_establish(&*server).await;
            server_send_bundle(
                &mut conn,
                [
                    WorkerTest::new(spec(1), INIT_RUN_NUMBER),
                    WorkerTest::new(spec(2), INIT_RUN_NUMBER),
                ],
                Eow(false),
            )
            .await;

            server_send_bundle(
                &mut conn,
                [
                    WorkerTest::new(spec(3), INIT_RUN_NUMBER),
                    WorkerTest::new(spec(4), INIT_RUN_NUMBER),
                ],
                Eow(true),
            )
            .await;
        };

        let fetch_task = async move {
            let NextWorkBundle { work, eow } = fetcher.get_next_tests().await.unwrap();
            assert_eq!(
                work,
                [
                    WorkerTest::new(spec(1), INIT_RUN_NUMBER),
                    WorkerTest::new(spec(2), INIT_RUN_NUMBER),
                ],
            );
            assert!(!eow);

            let NextWorkBundle { work, eow } = fetcher.get_next_tests().await.unwrap();
            assert_eq!(
                work,
                [
                    WorkerTest::new(spec(3), INIT_RUN_NUMBER),
                    WorkerTest::new(spec(4), INIT_RUN_NUMBER),
                ],
            );
            assert!(!eow, "should come back for retry manifest");
        };

        let ((), ()) = tokio::join!(server_task, fetch_task);
    }

    #[tokio::test]
    async fn fetch_retry_manifest_from_queue() {
        use out_of_process_retry_manifest_fetcher::test::{
            scaffold_server, server_establish, server_send_bundle,
        };

        let (server, queue_fetcher) = scaffold_server().await;
        let (mut fetcher, _results) = Fetcher::new_help(
            InitialSource::Retry(queue_fetcher),
            INIT_RUN_NUMBER,
            Duration::MAX,
        );

        let server_task = async move {
            let mut conn = server_establish(&*server).await;
            server_send_bundle(
                &mut conn,
                [
                    WorkerTest::new(spec(1), INIT_RUN_NUMBER),
                    WorkerTest::new(spec(2), INIT_RUN_NUMBER),
                    WorkerTest::new(spec(3), INIT_RUN_NUMBER),
                    WorkerTest::new(spec(4), INIT_RUN_NUMBER),
                ],
                Eow(true),
            )
            .await;
        };

        let fetch_task = async move {
            let NextWorkBundle { work, eow } = fetcher.get_next_tests().await.unwrap();
            assert_eq!(
                work,
                [
                    WorkerTest::new(spec(1), INIT_RUN_NUMBER),
                    WorkerTest::new(spec(2), INIT_RUN_NUMBER),
                    WorkerTest::new(spec(3), INIT_RUN_NUMBER),
                    WorkerTest::new(spec(4), INIT_RUN_NUMBER),
                ],
            );
            assert!(!eow, "should come back for retry manifest");
        };

        let ((), ()) = tokio::join!(server_task, fetch_task);
    }

    #[tokio::test]
    async fn fetch_from_queue_then_no_retries_fresh() {
        use persistent_test_fetcher::test::{
            scaffold_server, server_establish, server_send_bundle,
        };

        let (server, queue_fetcher) = scaffold_server(1).await;
        let (mut fetcher, _results) = Fetcher::new_help(
            InitialSource::Fresh(queue_fetcher),
            INIT_RUN_NUMBER,
            Duration::MAX,
        );

        let server_task = async move {
            let mut conn = server_establish(&*server).await;
            server_send_bundle(
                &mut conn,
                [
                    WorkerTest::new(spec(1), INIT_RUN_NUMBER),
                    WorkerTest::new(spec(2), INIT_RUN_NUMBER),
                ],
                Eow(true),
            )
            .await;
        };

        let fetch_task = async move {
            let NextWorkBundle { work, eow } = fetcher.get_next_tests().await.unwrap();
            assert_eq!(
                work,
                [
                    WorkerTest::new(spec(1), INIT_RUN_NUMBER),
                    WorkerTest::new(spec(2), INIT_RUN_NUMBER),
                ],
            );
            assert!(!eow);

            let NextWorkBundle { work, eow } = fetcher.get_next_tests().await.unwrap();
            assert_eq!(work, []);
            assert!(eow);
        };

        let ((), ()) = tokio::join!(server_task, fetch_task);
    }

    #[tokio::test]
    async fn fetch_from_queue_then_multiple_retries_fresh() {
        macro_rules! go_test {
            ($server:expr, $fetcher:expr, $results_tracker:expr) => {{
                let server_task = async move {
                    let mut conn = server_establish(&*$server).await;
                    server_send_bundle(
                        &mut conn,
                        [
                            WorkerTest::new(spec(1), INIT_RUN_NUMBER),
                            WorkerTest::new(spec(2), INIT_RUN_NUMBER),
                            WorkerTest::new(spec(3), INIT_RUN_NUMBER),
                        ],
                        Eow(true),
                    )
                    .await;
                };

                let fetch_task = async move {
                    // Attempt 1, from online
                    let NextWorkBundle { work, eow } = $fetcher.get_next_tests().await.unwrap();
                    assert_eq!(
                        work,
                        [
                            WorkerTest::new(spec(1), INIT_RUN_NUMBER),
                            WorkerTest::new(spec(2), INIT_RUN_NUMBER),
                            WorkerTest::new(spec(3), INIT_RUN_NUMBER),
                        ],
                    );
                    assert!(!eow);

                    $results_tracker.account_results([
                        &AssociatedTestResultsBuilder::new(
                            wid(1),
                            INIT_RUN_NUMBER,
                            [TestResultBuilder::new(test(1), FAILURE)],
                        )
                        .build(),
                        &AssociatedTestResultsBuilder::new(
                            wid(2),
                            INIT_RUN_NUMBER,
                            [TestResultBuilder::new(test(2), FAILURE)],
                        )
                        .build(),
                        &AssociatedTestResultsBuilder::new(
                            wid(3),
                            INIT_RUN_NUMBER,
                            [TestResultBuilder::new(test(3), SUCCESS)],
                        )
                        .build(),
                    ]);

                    // Attempt 2
                    let NextWorkBundle { work, eow } = $fetcher.get_next_tests().await.unwrap();
                    assert_eq!(
                        work,
                        [
                            WorkerTest::new(with_focus(spec(1), test(1)), INIT_RUN_NUMBER + 1),
                            WorkerTest::new(with_focus(spec(2), test(2)), INIT_RUN_NUMBER + 1),
                        ],
                    );
                    assert!(!eow);

                    $results_tracker.account_results([
                        &AssociatedTestResultsBuilder::new(
                            wid(1),
                            INIT_RUN_NUMBER + 1,
                            [TestResultBuilder::new(test(1), FAILURE)],
                        )
                        .build(),
                        &AssociatedTestResultsBuilder::new(
                            wid(2),
                            INIT_RUN_NUMBER + 1,
                            [TestResultBuilder::new(test(2), SUCCESS)],
                        )
                        .build(),
                    ]);

                    // Attempt 3
                    let NextWorkBundle { work, eow } = $fetcher.get_next_tests().await.unwrap();
                    assert_eq!(
                        work,
                        [WorkerTest::new(
                            with_focus(spec(1), test(1)),
                            INIT_RUN_NUMBER + 2
                        ),],
                    );
                    assert!(!eow);

                    $results_tracker.account_results([&AssociatedTestResultsBuilder::new(
                        wid(1),
                        INIT_RUN_NUMBER + 2,
                        [TestResultBuilder::new(test(1), FAILURE)],
                    )
                    .build()]);

                    // Done, even though we had failures
                    let NextWorkBundle { work, eow } = $fetcher.get_next_tests().await.unwrap();
                    assert_eq!(work, []);
                    assert!(eow);
                };

                let ((), ()) = tokio::join!(server_task, fetch_task);
            }};
        }

        {
            use persistent_test_fetcher::test::{
                scaffold_server, server_establish, server_send_bundle,
            };

            let (server, queue_fetcher) = scaffold_server(1).await;
            let (mut fetcher, mut results_tracker) = Fetcher::new_help(
                InitialSource::Fresh(queue_fetcher),
                INIT_RUN_NUMBER + 2,
                Duration::MAX,
            );

            go_test!(server, fetcher, results_tracker)
        }

        {
            use out_of_process_retry_manifest_fetcher::test::{
                scaffold_server, server_establish, server_send_bundle,
            };

            let (server, queue_fetcher) = scaffold_server().await;
            let (mut fetcher, mut results_tracker) = Fetcher::new_help(
                InitialSource::Retry(queue_fetcher),
                INIT_RUN_NUMBER + 2,
                Duration::MAX,
            );

            go_test!(server, fetcher, results_tracker)
        }
    }

    #[tokio::test]
    async fn fetch_from_queue_followed_by_end_of_work_waits_for_retries_fresh() {
        use persistent_test_fetcher::test::{
            scaffold_server, server_establish, server_send_bundle,
        };

        let (server, queue_fetcher) = scaffold_server(1).await;
        let (mut fetcher, mut results_tracker) = Fetcher::new_help(
            InitialSource::Fresh(queue_fetcher),
            INIT_RUN_NUMBER + 1,
            Duration::MAX,
        );

        let server_task = async move {
            let mut conn = server_establish(&*server).await;
            server_send_bundle(
                &mut conn,
                [WorkerTest::new(spec(1), INIT_RUN_NUMBER)],
                Eow(false),
            )
            .await;
            server_send_bundle(&mut conn, [], Eow(true)).await;
        };

        let fetch_task = async move {
            // Attempt 1, from online
            let NextWorkBundle { work, eow } = fetcher.get_next_tests().await.unwrap();
            assert_eq!(work, [WorkerTest::new(spec(1), INIT_RUN_NUMBER),],);
            assert!(!eow);

            // Attempt 2 will need to fetch from retries of attempt 1.

            results_tracker.account_results([&AssociatedTestResultsBuilder::new(
                wid(1),
                INIT_RUN_NUMBER,
                [TestResultBuilder::new(test(1), FAILURE)],
            )
            .build()]);

            // Attempt 2
            let NextWorkBundle { work, eow } = fetcher.get_next_tests().await.unwrap();
            assert_eq!(
                work,
                [WorkerTest::new(
                    with_focus(spec(1), test(1)),
                    INIT_RUN_NUMBER + 1
                )],
            );
            assert!(!eow);

            results_tracker.account_results([&AssociatedTestResultsBuilder::new(
                wid(1),
                INIT_RUN_NUMBER + 1,
                [TestResultBuilder::new(test(1), FAILURE)],
            )
            .build()]);

            // Done, even though we had failures
            let NextWorkBundle { work, eow } = fetcher.get_next_tests().await.unwrap();
            assert_eq!(work, []);
            assert!(eow);
        };

        let ((), ()) = tokio::join!(server_task, fetch_task);
    }

    #[tokio::test]
    async fn fetch_from_queue_followed_by_end_of_work_waits_for_retries_out_of_process_retry() {
        use out_of_process_retry_manifest_fetcher::test::{
            scaffold_server, server_establish, server_send_bundle,
        };

        let (server, queue_fetcher) = scaffold_server().await;
        let (mut fetcher, mut results_tracker) = Fetcher::new_help(
            InitialSource::Retry(queue_fetcher),
            INIT_RUN_NUMBER + 1,
            Duration::MAX,
        );

        let server_task = async move {
            let mut conn = server_establish(&*server).await;
            server_send_bundle(
                &mut conn,
                [WorkerTest::new(spec(1), INIT_RUN_NUMBER)],
                Eow(true),
            )
            .await;
        };

        let fetch_task = async move {
            // Attempt 1, from online
            let NextWorkBundle { work, eow } = fetcher.get_next_tests().await.unwrap();
            assert_eq!(work, [WorkerTest::new(spec(1), INIT_RUN_NUMBER),],);
            assert!(!eow);

            // Attempt 2 will need to fetch from retries of attempt 1.

            results_tracker.account_results([&AssociatedTestResultsBuilder::new(
                wid(1),
                INIT_RUN_NUMBER,
                [TestResultBuilder::new(test(1), FAILURE)],
            )
            .build()]);

            // Attempt 2
            let NextWorkBundle { work, eow } = fetcher.get_next_tests().await.unwrap();
            assert_eq!(
                work,
                [WorkerTest::new(
                    with_focus(spec(1), test(1)),
                    INIT_RUN_NUMBER + 1
                )],
            );
            assert!(!eow);

            results_tracker.account_results([&AssociatedTestResultsBuilder::new(
                wid(1),
                INIT_RUN_NUMBER + 1,
                [TestResultBuilder::new(test(1), FAILURE)],
            )
            .build()]);

            // Done, even though we had failures
            let NextWorkBundle { work, eow } = fetcher.get_next_tests().await.unwrap();
            assert!(work.is_empty());
            assert!(eow);
        };

        let ((), ()) = tokio::join!(server_task, fetch_task);
    }
}
