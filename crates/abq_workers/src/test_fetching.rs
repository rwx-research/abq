use std::net::SocketAddr;

use abq_generic_test_runner::TestsFetcher;
use abq_utils::{
    net_async::ConfiguredClient,
    net_protocol::{
        entity::Entity,
        workers::{NextWorkBundle, RunId},
    },
};
use async_trait::async_trait;
use tracing::instrument;

use self::persistent_test_fetcher::PersistedTestsFetcher;

mod persistent_test_fetcher;
mod retries;

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
/// execution of a test suite by a worker. Not yet implemented.
#[repr(transparent)]
pub struct Fetcher {
    initial_source: InitialSource,
}

impl Fetcher {
    pub fn new(
        entity: Entity,
        work_server_addr: SocketAddr,
        client: Box<dyn ConfiguredClient>,
        run_id: RunId,
    ) -> Self {
        Self {
            initial_source: InitialSource::Fresh(persistent_test_fetcher::start(
                entity,
                work_server_addr,
                client,
                run_id,
            )),
        }
    }
}

/// The initial source of tests; this is either FRESH or NEW_PROCESS_RETRY.
enum InitialSource {
    Fresh(PersistedTestsFetcher),
    // TODO: NEW_PROCESS_RETRY
}

#[async_trait]
impl TestsFetcher for Fetcher {
    #[instrument(level = "trace", skip(self))]
    async fn get_next_tests(&mut self) -> NextWorkBundle {
        match &mut self.initial_source {
            InitialSource::Fresh(fetcher) => fetcher.get_next_tests().await,
        }
    }
}
