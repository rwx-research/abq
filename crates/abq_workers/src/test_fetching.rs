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
    initial_sources_rev: Vec<InitialSource>,
    retry_source: RetryTracker,
    pending_retry_manifest_wait_time: Duration,
}

const DEFAULT_PENDING_RETRY_MANIFEST_WAIT_TIME: Duration = Duration::from_millis(100);

/// How the initial manifest from the queue should be sourced.
pub enum SourcingStrategy {
    Queue,
    RetryManifest,
}

impl Fetcher {
    pub fn new(
        sourcing_strategies: impl IntoIterator<Item = SourcingStrategy>,
        entity: Entity,
        work_server_addr: SocketAddr,
        client: Box<dyn ConfiguredClient>,
        run_id: RunId,
        max_run_number: u32,
    ) -> (Self, ResultsTracker) {
        let initial_source = InitialSource::from_strategies(
            sourcing_strategies,
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
        initial_sources: Vec<InitialSource>,
        max_run_number: u32,
        pending_retry_manifest_wait_time: Duration,
    ) -> (Self, ResultsTracker) {
        let (retry_source, results_tracker) = retries::build_tracking_pair(max_run_number);
        let me = Self {
            initial_sources_rev: initial_sources.into_iter().rev().collect(),
            retry_source,
            pending_retry_manifest_wait_time,
        };
        (me, results_tracker)
    }
}

enum InitialSource {
    Queue(PersistedTestsFetcher),
    ServerRetryManifest(OutOfProcessRetryManifestFetcher),
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
            SourcingStrategy::Queue => Self::Queue(persistent_test_fetcher::start(
                entity,
                work_server_addr,
                client,
                run_id,
            )),
            SourcingStrategy::RetryManifest => Self::ServerRetryManifest(
                OutOfProcessRetryManifestFetcher::new(entity, work_server_addr, client, run_id),
            ),
        }
    }

    fn from_strategies(
        strategies: impl IntoIterator<Item = SourcingStrategy>,
        entity: Entity,
        work_server_addr: SocketAddr,
        client: Box<dyn ConfiguredClient>,
        run_id: RunId,
    ) -> Vec<Self> {
        strategies
            .into_iter()
            .map(|strategy| {
                Self::from_strategy(
                    strategy,
                    entity,
                    work_server_addr,
                    client.boxed_clone(),
                    run_id.clone(),
                )
            })
            .collect()
    }
}

impl Fetcher {
    async fn fetch_next_tests(&mut self) -> Result<NextWorkBundle, FetchTestsError> {
        // Test fetching works in two phases:
        //
        //   1. Fetch the schedule of the manifest to be run by the worker from the queue.
        //      This schedule is used to hydrate the retry manifest tracker as it comes in.
        //   2. After the full manifest has been pulled, defer to the manifest tracker to supply
        //      all tests that should be re-run.
        //      This phase consists only of retries, and supplies tests to be run only once all
        //      test results in the last suite run attempt have been accounted for.
        //      One the retry manifest issues [NextWork::EndOfWork], there are no more tests to be
        //      run.
        loop {
            let initial_sources = &mut self.initial_sources_rev;

            match initial_sources.last_mut() {
                Some(source) => {
                    let mut tests = match source {
                        InitialSource::Queue(s) => {
                            tracing::debug!("Fetching from fresh manifest");
                            s.get_next_tests().await
                        }
                        InitialSource::ServerRetryManifest(s) => {
                            tracing::debug!("Fetching from retry manifest");
                            s.get_next_tests().await?
                        }
                    };

                    if tests.eow.0 {
                        self.initial_sources_rev.pop();
                    }
                    tests.eow = Eow(tests.eow.0 && self.initial_sources_rev.is_empty());

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
            vec![InitialSource::Queue(queue_fetcher)],
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
            vec![InitialSource::ServerRetryManifest(queue_fetcher)],
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
            vec![InitialSource::Queue(queue_fetcher)],
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
            vec![InitialSource::ServerRetryManifest(queue_fetcher)],
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
            vec![InitialSource::Queue(queue_fetcher)],
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
                vec![InitialSource::Queue(queue_fetcher)],
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
                vec![InitialSource::ServerRetryManifest(queue_fetcher)],
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
            vec![InitialSource::Queue(queue_fetcher)],
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
            vec![InitialSource::ServerRetryManifest(queue_fetcher)],
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

    #[tokio::test]
    async fn fetch_retry_manifest_then_fetch_from_queue() {
        use out_of_process_retry_manifest_fetcher::test as rm;
        use persistent_test_fetcher::test as pf;

        let (retry_server, retry_fetcher) = rm::scaffold_server().await;
        let (queue_server, queue_fetcher) = pf::scaffold_server(1).await;
        let (mut fetcher, mut results_tracker) = Fetcher::new_help(
            vec![
                InitialSource::ServerRetryManifest(retry_fetcher),
                InitialSource::Queue(queue_fetcher),
            ],
            INIT_RUN_NUMBER + 1,
            Duration::MAX,
        );

        let retry_sender_task = async move {
            for i in 0..2 {
                let mut conn = rm::server_establish(&*retry_server).await;
                rm::server_send_bundle(
                    &mut conn,
                    [WorkerTest::new(spec(i + 1), INIT_RUN_NUMBER)],
                    Eow(i == 1),
                )
                .await;
            }
        };

        let queue_sender_task = async move {
            let mut conn = pf::server_establish(&*queue_server).await;
            for i in 0..2 {
                pf::server_send_bundle(
                    &mut conn,
                    [WorkerTest::new(spec(i + 3), INIT_RUN_NUMBER)],
                    Eow(i == 1),
                )
                .await;
            }
        };

        let fetch_task = async move {
            let NextWorkBundle { work, eow } = fetcher.get_next_tests().await.unwrap();
            assert_eq!(work, [WorkerTest::new(spec(1), INIT_RUN_NUMBER)]);
            assert!(!eow, "should come back for server retry manifest");

            let NextWorkBundle { work, eow } = fetcher.get_next_tests().await.unwrap();
            assert_eq!(work, [WorkerTest::new(spec(2), INIT_RUN_NUMBER)]);
            assert!(!eow, "should come back for online queue");

            let NextWorkBundle { work, eow } = fetcher.get_next_tests().await.unwrap();
            assert_eq!(work, [WorkerTest::new(spec(3), INIT_RUN_NUMBER)]);
            assert!(!eow, "should come back for online queue");

            let NextWorkBundle { work, eow } = fetcher.get_next_tests().await.unwrap();
            assert_eq!(work, [WorkerTest::new(spec(4), INIT_RUN_NUMBER)]);
            assert!(!eow, "should come back for online queue");

            for i in 0..4 {
                results_tracker.account_results([&AssociatedTestResultsBuilder::new(
                    wid(i + 1),
                    INIT_RUN_NUMBER,
                    [TestResultBuilder::new(test(i + 1), SUCCESS)],
                )
                .build()]);
            }

            let NextWorkBundle { work, eow } = fetcher.get_next_tests().await.unwrap();
            assert!(work.is_empty());
            assert!(eow, "should have nothing to retry");
        };

        tokio::join!(retry_sender_task, queue_sender_task, fetch_task);
    }
}
