//! Implements a test fetcher that fetches a retry manifest in one shot from the queue.

use std::{net::SocketAddr, time::Duration};

use abq_utils::{
    net_async::ConfiguredClient,
    net_protocol::{
        self,
        entity::Entity,
        workers::{NextWorkBundle, RunId},
    },
    retry::async_retry_n,
};

const CONNECTION_ATTEMPTS: usize = 5;
const CONNECTION_BACKOFF: Duration = Duration::from_secs(3);

const TRY_FETCH_MANIFEST_ATTEMPTS: usize = 20;
const TRY_FETCH_MANIFEST_BACKOFF: Duration = Duration::from_secs(3);
const TRY_FETCH_MANIFEST_SECONDS: usize =
    TRY_FETCH_MANIFEST_BACKOFF.as_secs() as usize * TRY_FETCH_MANIFEST_ATTEMPTS;

pub struct OutOfProcessRetryManifestFetcher {
    entity: Entity,
    work_server_addr: SocketAddr,
    client: Box<dyn ConfiguredClient>,
    run_id: RunId,
}

impl OutOfProcessRetryManifestFetcher {
    pub fn new(
        entity: Entity,
        work_server_addr: SocketAddr,
        client: Box<dyn ConfiguredClient>,
        run_id: RunId,
    ) -> Self {
        Self {
            entity,
            work_server_addr,
            client,
            run_id,
        }
    }
}

impl OutOfProcessRetryManifestFetcher {
    pub async fn get_next_tests(self) -> NextWorkBundle {
        let Self {
            entity,
            work_server_addr,
            client,
            run_id,
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
                Manifest(manifest) => Ok(manifest),
                NotYetPersisted => Err(()),
                RunDoesNotExist => unreachable!("attempted to request a retry manifest for a test run that does not exist on the target queue"),
                // TODO make all of these errors
                ManifestNeverReceived => unreachable!("manifest never received for the given run"),
                FailedToLoad => unreachable!("manifest failed to be loaded by the queue"),
            }
        };

        async_retry_n(
            TRY_FETCH_MANIFEST_ATTEMPTS,
            TRY_FETCH_MANIFEST_BACKOFF,
            try_fetch_manifest,
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "failed to fetch manifest after {} seconds",
                TRY_FETCH_MANIFEST_SECONDS
            )
        })
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
            work_server::{Message, Request, RetryManifestResponse},
            workers::{Eow, NextWorkBundle, RunId, WorkerTest},
        },
        tls::{ClientTlsStrategy, ServerTlsStrategy},
    };

    use super::OutOfProcessRetryManifestFetcher;

    pub async fn scaffold_server() -> (Box<dyn ServerListener>, OutOfProcessRetryManifestFetcher) {
        let server_opts =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
        let client_opts =
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());

        let listener = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let client = client_opts.build_async().unwrap();
        let server_addr = listener.local_addr().unwrap();

        let fetcher = OutOfProcessRetryManifestFetcher::new(
            Entity::fake(),
            server_addr,
            client,
            RunId::unique(),
        );

        (listener, fetcher)
    }

    pub async fn server_send_bundle(
        conn: &mut Box<dyn ServerStream>,
        bundle: impl IntoIterator<Item = WorkerTest>,
        eow: Eow,
    ) {
        let request: Request = async_read(conn).await.unwrap();
        assert!(matches!(
            request.message,
            Message::RetryManifestPartition { .. }
        ));
        async_write(
            conn,
            &RetryManifestResponse::Manifest(NextWorkBundle {
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
    async fn smoke() {
        let (server, fetcher) = scaffold_server().await;

        let server_task = async move {
            let mut conn = server_establish(&*server).await;
            server_send_bundle(&mut conn, [], Eow(true)).await;
        };

        let client_task = async move {
            let bundle = fetcher.get_next_tests().await;
            assert!(bundle.work.is_empty());
            assert!(bundle.eow.0);
        };

        let ((), ()) = tokio::join!(server_task, client_task);
    }
}
