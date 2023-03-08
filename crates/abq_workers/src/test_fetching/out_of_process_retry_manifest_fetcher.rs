//! Implements a test fetcher that fetches a retry manifest in one shot from the queue.

use std::{net::SocketAddr, time::Duration};

use abq_utils::{
    net_async::ConfiguredClient,
    net_protocol::{
        self,
        entity::Entity,
        error::FetchTestsError,
        workers::{NextWorkBundle, RunId},
    },
    retry::async_retry_n,
};

const CONNECTION_ATTEMPTS: usize = 5;
const CONNECTION_BACKOFF: Duration = Duration::from_secs(3);

const TRY_FETCH_MANIFEST_ATTEMPTS: usize = 20;
const TRY_FETCH_MANIFEST_BACKOFF: Duration = Duration::from_secs(3);

pub struct OutOfProcessRetryManifestFetcher {
    entity: Entity,
    work_server_addr: SocketAddr,
    client: Box<dyn ConfiguredClient>,
    run_id: RunId,
    fetch_manifest_attempts: usize,
    fetch_manifest_backoff: Duration,
}

impl OutOfProcessRetryManifestFetcher {
    pub fn new(
        entity: Entity,
        work_server_addr: SocketAddr,
        client: Box<dyn ConfiguredClient>,
        run_id: RunId,
    ) -> Self {
        Self::new_help(
            entity,
            work_server_addr,
            client,
            run_id,
            TRY_FETCH_MANIFEST_ATTEMPTS,
            TRY_FETCH_MANIFEST_BACKOFF,
        )
    }

    pub fn new_help(
        entity: Entity,
        work_server_addr: SocketAddr,
        client: Box<dyn ConfiguredClient>,
        run_id: RunId,
        fetch_manifest_attempts: usize,
        fetch_manifest_backoff: Duration,
    ) -> Self {
        Self {
            entity,
            work_server_addr,
            client,
            run_id,
            fetch_manifest_attempts,
            fetch_manifest_backoff,
        }
    }
}

impl OutOfProcessRetryManifestFetcher {
    pub async fn get_next_tests(self) -> Result<NextWorkBundle, FetchTestsError> {
        let Self {
            entity,
            work_server_addr,
            client,
            run_id,
            fetch_manifest_attempts,
            fetch_manifest_backoff,
        } = self;

        let try_fetch_manifest = |_attempt| async {
            let mut conn = async_retry_n(CONNECTION_ATTEMPTS, CONNECTION_BACKOFF, |attempt| {
                if attempt > 1 {
                    tracing::info!(
                        "reattempting connection to work server for retry manifest {}",
                        attempt
                    );
                }
                client.connect(work_server_addr)
            })
            .await
            .expect("work server not available");

            use net_protocol::work_server::{Message, Request, RetryManifestResponse::*};

            let request = Request {
                entity,
                message: Message::RetryManifestPartition {
                    run_id: run_id.clone(),
                    entity,
                },
            };
            net_protocol::async_write(&mut conn, &request)
                .await
                .unwrap();

            match net_protocol::async_read(&mut conn).await.unwrap() {
                Manifest(manifest) => Ok(Ok(manifest)),
                NotYetPersisted => Err(()), // retry
                Error(e) => Ok(Err(e.into())),
            }
        };

        async_retry_n(
            fetch_manifest_attempts,
            fetch_manifest_backoff,
            try_fetch_manifest,
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "failed to fetch manifest after {} seconds",
                fetch_manifest_attempts * (fetch_manifest_backoff.as_secs() as usize)
            )
        })
    }
}

#[cfg(test)]
pub mod test {
    use std::time::Duration;

    use abq_utils::{
        auth::{ClientAuthStrategy, ServerAuthStrategy},
        net_async::{ServerListener, ServerStream},
        net_opt::{ClientOptions, ServerOptions},
        net_protocol::{
            async_read, async_write,
            entity::Entity,
            error::{FetchTestsError, RetryManifestError},
            work_server::{Message, Request, RetryManifestResponse},
            workers::{Eow, NextWorkBundle, RunId, WorkerTest},
        },
        tls::{ClientTlsStrategy, ServerTlsStrategy},
    };

    use super::{
        OutOfProcessRetryManifestFetcher, TRY_FETCH_MANIFEST_ATTEMPTS, TRY_FETCH_MANIFEST_BACKOFF,
    };

    pub async fn scaffold_server() -> (Box<dyn ServerListener>, OutOfProcessRetryManifestFetcher) {
        scaffold_server_help(TRY_FETCH_MANIFEST_ATTEMPTS, TRY_FETCH_MANIFEST_BACKOFF).await
    }

    async fn scaffold_server_help(
        fetch_manifest_attempts: usize,
        fetch_manifest_backoff: Duration,
    ) -> (Box<dyn ServerListener>, OutOfProcessRetryManifestFetcher) {
        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let listener = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let client = client_opts.build_async().unwrap();
        let server_addr = listener.local_addr().unwrap();

        let fetcher = OutOfProcessRetryManifestFetcher::new_help(
            Entity::fake(),
            server_addr,
            client,
            RunId::unique(),
            fetch_manifest_attempts,
            fetch_manifest_backoff,
        );

        (listener, fetcher)
    }

    pub async fn server_send_bundle(
        conn: &mut Box<dyn ServerStream>,
        bundle: impl IntoIterator<Item = WorkerTest>,
        eow: Eow,
    ) {
        server_send_response(
            conn,
            &RetryManifestResponse::Manifest(NextWorkBundle {
                work: bundle.into_iter().collect(),
                eow,
            }),
        )
        .await
    }

    async fn server_send_response(
        conn: &mut Box<dyn ServerStream>,
        response: &RetryManifestResponse,
    ) {
        let request: Request = async_read(conn).await.unwrap();
        assert!(matches!(
            request.message,
            Message::RetryManifestPartition { .. }
        ));
        async_write(conn, response).await.unwrap();
    }

    pub async fn server_establish(server: &dyn ServerListener) -> Box<dyn ServerStream> {
        let (conn, _) = server.accept().await.unwrap();
        server.handshake_ctx().handshake(conn).await.unwrap()
    }

    #[tokio::test]
    async fn smoke() {
        let (server, fetcher) = scaffold_server().await;

        let server_task = async move {
            let mut conn = server_establish(&*server).await;
            server_send_bundle(&mut conn, [], Eow(true)).await;
        };

        let client_task = async move {
            let bundle = fetcher.get_next_tests().await.unwrap();
            assert!(bundle.work.is_empty());
            assert!(bundle.eow.0);
        };

        let ((), ()) = tokio::join!(server_task, client_task);
    }

    #[tokio::test]
    async fn pending_finally_succeeds() {
        let (server, fetcher) = scaffold_server_help(3, Duration::ZERO).await;

        let server_task = async move {
            // Accept, then say pending manifest
            for _ in 0..2 {
                let mut conn = server_establish(&*server).await;
                server_send_response(&mut conn, &RetryManifestResponse::NotYetPersisted).await;
            }

            // The third time, let it through
            let mut conn = server_establish(&*server).await;
            server_send_bundle(&mut conn, [], Eow(true)).await;
        };

        let client_task = async move {
            let bundle = fetcher.get_next_tests().await.unwrap();
            assert!(bundle.work.is_empty());
            assert!(bundle.eow);
        };

        let ((), ()) = tokio::join!(server_task, client_task);
    }

    macro_rules! test_error_out_on_errors {
        ($($test:ident, $error:path)+) => {
            fn _check_exhaustive(e: RetryManifestError) {
                match e {
                    $($error => {})+
                }
            }

            $(
            #[tokio::test]
            async fn $test() {
                let (server, fetcher) = scaffold_server().await;

                let server_task = async move {
                    let mut conn = server_establish(&*server).await;
                    server_send_response(
                        &mut conn,
                        &RetryManifestResponse::Error($error),
                    )
                    .await;
                };

                let client_task = async move {
                    let result = fetcher.get_next_tests().await;
                    assert_eq!(result, Err(FetchTestsError::RetryManifest($error)));
                };

                let ((), ()) = tokio::join!(server_task, client_task);
            }
            )+
        };
    }

    test_error_out_on_errors!(
        error_on_cancelled, RetryManifestError::RunCancelled
        error_on_manifest_missing, RetryManifestError::ManifestNeverReceived
        error_on_failed_to_load, RetryManifestError::FailedToLoad
        error_on_run_does_not_exist, RetryManifestError::RunDoesNotExist
    );
}
