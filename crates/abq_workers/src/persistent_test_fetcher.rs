//! Implements a persistent test-fetching connection from workers to the queue.

use std::{io, net::SocketAddr};

use crate::workers::{GetNextTests, TestsFetcher};
use abq_utils::{
    net_async::{ClientStream, ConfiguredClient},
    net_protocol::{
        self,
        entity::Entity,
        workers::{NextWorkBundle, RunId},
    },
};
use async_trait::async_trait;

/// Returns a [GetNextTests] implementation which operates by keeping a persistent connection to fetch
/// next tests open with the work server.
pub(crate) fn start(
    entity: Entity,
    work_server_addr: SocketAddr,
    client: Box<dyn ConfiguredClient>,
    run_id: RunId,
) -> GetNextTests {
    Box::new(PersistedTestsFetcher {
        entity,
        work_server_addr,
        client,
        conn: Default::default(),
        run_id,
    })
}

pub(crate) struct PersistedTestsFetcher {
    entity: Entity,
    work_server_addr: SocketAddr,
    client: Box<dyn ConfiguredClient>,
    conn: PersistedConnection,
    run_id: RunId,
}

type PersistedConnection = Option<Box<dyn ClientStream>>;

const MAX_TRIES_FOR_ONE_CYCLE: usize = 3;

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
                    if attempt < MAX_TRIES_FOR_ONE_CYCLE {
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
