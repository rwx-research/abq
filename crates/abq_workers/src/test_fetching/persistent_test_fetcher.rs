//! Implements a persistent test-fetching connection from workers to the queue.

use std::{io, net::SocketAddr};

use abq_utils::{
    net_async::{ClientStream, ConfiguredClient},
    net_protocol::{
        self,
        entity::Entity,
        workers::{NextWorkBundle, RunId},
    },
};
use tracing::instrument;

const DEFAULT_MAX_ATTEMPTS_IN_CYCLE: usize = 3;

/// Returns a [GetNextTests] implementation which operates by keeping a persistent connection to fetch
/// next tests open with the work server.
pub fn start(
    entity: Entity,
    work_server_addr: SocketAddr,
    client: Box<dyn ConfiguredClient>,
    run_id: RunId,
) -> PersistedTestsFetcher {
    PersistedTestsFetcher::new(
        entity,
        work_server_addr,
        client,
        run_id,
        DEFAULT_MAX_ATTEMPTS_IN_CYCLE,
    )
}

pub struct PersistedTestsFetcher {
    entity: Entity,
    work_server_addr: SocketAddr,
    client: Box<dyn ConfiguredClient>,
    conn: PersistedConnection,
    run_id: RunId,
    max_attempts_in_cycle: usize,
}

type PersistedConnection = Option<Box<dyn ClientStream>>;

impl PersistedTestsFetcher {
    fn new(
        entity: Entity,
        work_server_addr: SocketAddr,
        client: Box<dyn ConfiguredClient>,
        run_id: RunId,
        max_attempts_in_cycle: usize,
    ) -> Self {
        Self {
            entity,
            work_server_addr,
            client,
            conn: Default::default(),
            run_id,
            max_attempts_in_cycle,
        }
    }
}

impl PersistedTestsFetcher {
    #[instrument(level = "trace", skip_all, fields(run_id=?self.run_id, work_server=?self.work_server_addr))]
    pub async fn get_next_tests(&mut self) -> NextWorkBundle {
        // TODO: propagate errors here upwards rather than panicking
        self.wait_for_next_work_bundle().await.unwrap()
    }

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

        // Make sure that if our connection is closed, we re-open it.
        let conn = self.ensure_persistent_conn().await?;

        let next_test_request = net_protocol::work_server::NextTestRequest {};
        net_protocol::async_write(&mut *conn, &next_test_request).await?;
        match net_protocol::async_read(&mut *conn).await? {
            NextTestResponse::Bundle(bundle) => Ok(bundle),
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
pub mod test {
    use abq_utils::{
        auth::{ClientAuthStrategy, ServerAuthStrategy},
        net_async::{ServerListener, ServerStream},
        net_opt::{ClientOptions, ServerOptions},
        net_protocol::{
            async_read, async_write,
            entity::Entity,
            work_server::{NextTestRequest, NextTestResponse},
            workers::{Eow, NextWorkBundle, RunId, WorkerTest},
        },
        tls::{ClientTlsStrategy, ServerTlsStrategy},
    };

    use super::PersistedTestsFetcher;

    pub async fn scaffold_server(
        max_attempts_in_cycle: usize,
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
        );

        (listener, fetcher)
    }

    pub async fn server_send_bundle(
        conn: &mut Box<dyn ServerStream>,
        bundle: impl IntoIterator<Item = WorkerTest>,
        eow: Eow,
    ) {
        let NextTestRequest {} = async_read(conn).await.unwrap();
        async_write(
            conn,
            &NextTestResponse::Bundle(NextWorkBundle {
                work: bundle.into_iter().collect(),
                eow,
            }),
        )
        .await
        .unwrap();
    }

    pub async fn server_establish(server: &dyn ServerListener) -> Box<dyn ServerStream> {
        let (conn, _) = server.accept().await.unwrap();
        server.handshake_ctx().handshake(conn).await.unwrap()
    }

    #[tokio::test]
    async fn retry_on_connection_failure_finally_succeeds() {
        let (server, mut fetcher) = scaffold_server(3).await;

        let server_task = async move {
            // Accept, then drop the connection
            for _ in 0..2 {
                let (conn, _) = server.accept().await.unwrap();
                drop(conn);
            }

            // The third time, let it through
            let mut conn = server_establish(&*server).await;
            server_send_bundle(&mut conn, [], Eow(true)).await;
        };

        let client_task = async move {
            let bundle = fetcher.wait_for_next_work_bundle().await.unwrap();
            assert!(bundle.work.is_empty());
        };

        let ((), ()) = tokio::join!(server_task, client_task);
    }

    #[tokio::test]
    async fn retry_on_connection_failure_finally_fails() {
        let (server, mut fetcher) = scaffold_server(3).await;

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
}
