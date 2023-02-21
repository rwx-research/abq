//! Implements a persistent test-fetching connection from workers to the queue.

use std::{io, net::SocketAddr, time::Duration};

use crate::workers::{GetNextTests, TestsFetcher};
use abq_utils::{
    decay::ExpDecay,
    net_async::{ClientStream, ConfiguredClient},
    net_protocol::{
        self,
        entity::Entity,
        workers::{NextWorkBundle, RunId},
    },
};
use async_trait::async_trait;

const DEFAULT_MAX_ATTEMPTS_IN_CYCLE: usize = 3;

// If the next tests are pending, use a quadratic decay in waiting for them to populate.
// This is because we might be at the very tail of the job queue, and waiting for any
// retries (if they exist) to re-populate, which may happen very quickly.
const DEFAULT_DECAY_ON_PENDING_TESTS_NOTIFICATION: ExpDecay =
    ExpDecay::quadratic(Duration::from_millis(20), Duration::from_secs(3));

/// Returns a [GetNextTests] implementation which operates by keeping a persistent connection to fetch
/// next tests open with the work server.
pub(crate) fn start(
    entity: Entity,
    work_server_addr: SocketAddr,
    client: Box<dyn ConfiguredClient>,
    run_id: RunId,
) -> GetNextTests {
    Box::new(PersistedTestsFetcher::new(
        entity,
        work_server_addr,
        client,
        run_id,
        DEFAULT_MAX_ATTEMPTS_IN_CYCLE,
        DEFAULT_DECAY_ON_PENDING_TESTS_NOTIFICATION,
    ))
}

pub(crate) struct PersistedTestsFetcher {
    entity: Entity,
    work_server_addr: SocketAddr,
    client: Box<dyn ConfiguredClient>,
    conn: PersistedConnection,
    run_id: RunId,
    max_attempts_in_cycle: usize,
    decay_on_pending_tests_notification: ExpDecay,
}

type PersistedConnection = Option<Box<dyn ClientStream>>;

impl PersistedTestsFetcher {
    fn new(
        entity: Entity,
        work_server_addr: SocketAddr,
        client: Box<dyn ConfiguredClient>,
        run_id: RunId,
        max_attempts_in_cycle: usize,
        decay_on_pending_tests_notification: ExpDecay,
    ) -> Self {
        Self {
            entity,
            work_server_addr,
            client,
            conn: Default::default(),
            run_id,
            max_attempts_in_cycle,
            decay_on_pending_tests_notification,
        }
    }
}

#[async_trait]
impl TestsFetcher for PersistedTestsFetcher {
    async fn get_next_tests(&mut self) -> NextWorkBundle {
        let span = tracing::trace_span!("get_next_tests", run_id=?self.run_id, work_server=?self.work_server_addr);
        let _get_next_work = span.enter();

        // TODO: propagate errors here upwards rather than panicking
        self.wait_for_next_work_bundle().await.unwrap()
    }
}

impl PersistedTestsFetcher {
    /// Asks the work server for the next item of work to run.
    ///
    /// If the connection is noted to have been dropped, we re-try at most twice.
    async fn wait_for_next_work_bundle(&mut self) -> io::Result<NextWorkBundle> {
        // As mentioned above (see PersistedConnection), the lock here is mostly ceremonious.
        // Take it for the entirety of the fetch to avoid needless contention with ourselves.
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.try_request().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if attempt < self.max_attempts_in_cycle {
                        tracing::warn!(?attempt, ?self.run_id, "Retrying fetch of work bundle");
                        // Assume the connection was dropped, and force a re-connect.
                        self.conn = None;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn try_request(&mut self) -> Result<NextWorkBundle, io::Error> {
        use net_protocol::work_server::NextTestResponse;

        let mut decay = self.decay_on_pending_tests_notification;

        loop {
            // Make sure that if our connection is closed, we re-open it.
            let conn = self.ensure_persistent_conn().await?;

            let next_test_request = net_protocol::work_server::NextTestRequest {};
            net_protocol::async_write(&mut *conn, &next_test_request).await?;
            match net_protocol::async_read(&mut *conn).await? {
                NextTestResponse::Bundle(bundle) => return Ok(bundle),
                NextTestResponse::Pending => {
                    tracing::debug!(entity=?self.entity, "waiting for pending next tests");

                    // Sleep, then try again, since we were told to wait.
                    tokio::time::sleep(decay.next_duration()).await;
                }
            }
        }
    }

    async fn ensure_persistent_conn(&mut self) -> io::Result<&mut Box<dyn ClientStream>> {
        use net_protocol::work_server::{Message, Request};

        match &mut self.conn {
            Some(conn) => Ok(conn),
            my_conn @ None => {
                let mut conn = self.client.connect(self.work_server_addr).await?;
                let request = Request {
                    entity: self.entity,
                    message: Message::PersistentWorkerNextTestsConnection(self.run_id.clone()),
                };
                net_protocol::async_write(&mut conn, &request).await?;

                *my_conn = Some(conn);
                Ok(my_conn.as_mut().unwrap())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use abq_utils::{
        auth::{ClientAuthStrategy, ServerAuthStrategy},
        decay::ExpDecay,
        net_async::ServerListener,
        net_opt::{ClientOptions, ServerOptions},
        net_protocol::{
            async_read, async_write,
            entity::Entity,
            work_server::{NextTestRequest, NextTestResponse},
            workers::{NextWorkBundle, RunId},
        },
        tls::{ClientTlsStrategy, ServerTlsStrategy},
    };

    use super::PersistedTestsFetcher;

    async fn scaffold(
        max_attempts_in_cycle: usize,
        decay_on_pending_tests: ExpDecay,
    ) -> (Box<dyn ServerListener>, PersistedTestsFetcher) {
        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let listener = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let client = client_opts.build_async().unwrap();
        let server_addr = listener.local_addr().unwrap();

        let fetcher = PersistedTestsFetcher::new(
            Entity::fake(),
            server_addr,
            client,
            RunId::unique(),
            max_attempts_in_cycle,
            decay_on_pending_tests,
        );

        (listener, fetcher)
    }

    #[tokio::test]
    async fn wait_while_pending_tests_notification() {
        let (server, mut fetcher) =
            scaffold(1, ExpDecay::constant(Duration::from_micros(10))).await;

        let server_task = async move {
            let (conn, _) = server.accept().await.unwrap();
            let mut conn = server.handshake_ctx().handshake(conn).await.unwrap();
            for _ in 0..10 {
                let NextTestRequest {} = async_read(&mut conn).await.unwrap();
                async_write(&mut conn, &NextTestResponse::Pending)
                    .await
                    .unwrap();
            }

            let NextTestRequest {} = async_read(&mut conn).await.unwrap();
            async_write(
                &mut conn,
                &NextTestResponse::Bundle(NextWorkBundle::new(vec![])),
            )
            .await
            .unwrap();
        };

        let client_task = async move {
            let bundle = fetcher.wait_for_next_work_bundle().await.unwrap();
            assert!(bundle.work.is_empty());
        };

        let ((), ()) = tokio::join!(server_task, client_task);
    }

    #[tokio::test]
    async fn retry_on_connection_failure_finally_succeeds() {
        let (server, mut fetcher) = scaffold(3, ExpDecay::constant(Duration::MAX)).await;

        let server_task = async move {
            // Accept, then drop the connection
            for _ in 0..2 {
                let (conn, _) = server.accept().await.unwrap();
                drop(conn);
            }

            // The third time, let it through
            let (conn, _) = server.accept().await.unwrap();
            let mut conn = server.handshake_ctx().handshake(conn).await.unwrap();
            let NextTestRequest {} = async_read(&mut conn).await.unwrap();
            async_write(
                &mut conn,
                &NextTestResponse::Bundle(NextWorkBundle::new(vec![])),
            )
            .await
            .unwrap();
        };

        let client_task = async move {
            let bundle = fetcher.wait_for_next_work_bundle().await.unwrap();
            assert!(bundle.work.is_empty());
        };

        let ((), ()) = tokio::join!(server_task, client_task);
    }

    #[tokio::test]
    async fn retry_on_connection_failure_finally_fails() {
        let (server, mut fetcher) = scaffold(3, ExpDecay::constant(Duration::MAX)).await;

        let server_task = async move {
            // Accept, then drop the connection all three times.
            for _ in 0..3 {
                let (conn, _) = server.accept().await.unwrap();
                drop(conn);
            }
        };

        let client_task = async move {
            let result = fetcher.wait_for_next_work_bundle().await;
            assert!(result.is_err());
        };

        let ((), ()) = tokio::join!(server_task, client_task);
    }

    #[tokio::test]
    async fn wait_while_pending_tests_notification_uses_fresh_decay() {
        let decay = ExpDecay::quadratic(Duration::from_micros(10), Duration::from_micros(640));
        let (server, mut fetcher) = scaffold(1, decay).await;

        let server_task = async move {
            let (conn, _) = server.accept().await.unwrap();
            let mut conn = server.handshake_ctx().handshake(conn).await.unwrap();
            for _ in 0..2 {
                let NextTestRequest {} = async_read(&mut conn).await.unwrap();
                async_write(&mut conn, &NextTestResponse::Pending)
                    .await
                    .unwrap();
            }

            let NextTestRequest {} = async_read(&mut conn).await.unwrap();
            async_write(
                &mut conn,
                &NextTestResponse::Bundle(NextWorkBundle::new(vec![])),
            )
            .await
            .unwrap();
        };

        let client_task = async move {
            let bundle = fetcher.wait_for_next_work_bundle().await.unwrap();
            assert!(bundle.work.is_empty());
            fetcher
        };

        let ((), mut fetcher) = tokio::join!(server_task, client_task);

        // Decay should still be at the starting state.
        assert_eq!(
            fetcher
                .decay_on_pending_tests_notification
                .next_duration()
                .as_micros(),
            10
        );
    }
}
