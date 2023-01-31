//! Persisted connections between the queue and workers.

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use abq_utils::net_protocol::workers::RunId;
use parking_lot::Mutex;
use tokio::{sync::oneshot, task::JoinSet};

use crate::prelude::{AnyError, OpaqueResult};

#[derive(Clone)]
pub(crate) struct ConnectedWorkers {
    timeout: Duration,
    map: Arc<
        Mutex<
            //
            HashMap<RunId, PersistedConnectionSet>,
        >,
    >,
}

#[derive(Default)]
struct PersistedConnectionSet {
    // NB: we do not care about the ordering of launched tasks, so we prefer
    // provided collections here over ordered ones.
    all_tasks: JoinSet<OpaqueResult<()>>,
    all_tx_stop: Vec<oneshot::Sender<()>>,
}

const DEFAULT_STOP_TIMEOUT: Duration = Duration::from_secs(10);

impl Default for ConnectedWorkers {
    fn default() -> Self {
        Self::new(DEFAULT_STOP_TIMEOUT)
    }
}

pub(crate) enum StopResult {
    /// The run ID was not associated in the connection set.
    RunNotAssociated,
    /// Stopped worker connection tasks for a run ID. Returns errors seen during stopping, if any.
    Stopped(Vec<AnyError>),
}

impl ConnectedWorkers {
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            map: Default::default(),
        }
    }

    /// Launches a task responsible for communicating with a persisted worker connection for a run.
    /// This method will call `spawn` itself; you should not do so yourself.
    ///
    /// If there is not already an association for the run ID, a new one will be created. This is
    /// to avoid locking of the state machine of test runs for associating a test run ID.
    ///
    /// # Invariants
    ///
    /// - Must be called in an async (tokio) runtime
    pub fn insert<F, Fut>(&self, run_id: RunId, create_task: F)
    where
        F: FnOnce(/* on stop */ oneshot::Receiver<()>) -> Fut,
        Fut: Future<Output = OpaqueResult<()>> + Send + 'static,
    {
        let (tx_stop, rx_stop) = oneshot::channel();
        let task = create_task(rx_stop);
        {
            let mut map = self.map.lock();
            let conn_set = map.entry(run_id).or_default();

            // We manage task cancellation via channels, so no need for the forcable abort handles.
            let _abort_handle = conn_set.all_tasks.spawn(task);
            conn_set.all_tx_stop.push(tx_stop);
        }
    }

    /// Stop the set of persisted worker connections associated with a test run.
    /// Calls the stop-oneshot provided to each task [upon creation][Self::create].
    ///
    /// Resolves only once all worker connections are stopped, or the timeout for
    /// stopping is reached. If the timeout for stopping is reached, the tasks are
    /// forcibly aborted.
    pub async fn stop(&self, run_id: &RunId) -> StopResult {
        // NB: take exclusive access over the map only for as long as removal, not shutdown, takes.
        let removed_set = { self.map.lock().remove(run_id) };
        let PersistedConnectionSet {
            mut all_tasks,
            all_tx_stop,
        } = match removed_set {
            Some(set) => set,
            None => return StopResult::RunNotAssociated,
        };

        for tx_stop in all_tx_stop {
            // The task can exit of its own volition prior to receiving the stop signal.
            let _opt_err = tx_stop.send(());
        }

        let wait_for_all_tasks = async {
            let mut errors: Vec<AnyError> = vec![];
            while let Some(join_result) = all_tasks.join_next().await {
                match join_result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => errors.push(e),
                    Err(e) => errors.push(e.into()),
                }
            }
            errors
        };

        tokio::select! {
            result = wait_for_all_tasks => {
                StopResult::Stopped(result)
            }
            _ = tokio::time::sleep(self.timeout) => {
                tracing::error!(?run_id, timeout=?self.timeout, "forcing abort for worker connections failing to exit");
                all_tasks.shutdown().await;
                StopResult::Stopped(
                    vec!["Forced abort for worker connections".into()]
                )
            }
        }
    }

    #[cfg(test)]
    pub fn has_tasks_for(&self, run_id: &RunId) -> bool {
        self.map.lock().contains_key(run_id)
    }
}

#[cfg(test)]
mod test {
    use std::{future::Future, sync::Arc, time::Duration};

    use abq_utils::net_protocol::workers::RunId;
    use tokio::sync::Mutex;

    use crate::{connections::StopResult, prelude::AnyError};

    use super::ConnectedWorkers;

    const MULTIPLE_WORKERS: usize = 20;
    const MULTIPLE_RUN_IDS: usize = 20;

    type IdVec = Arc<Mutex<Vec<usize>>>;

    async fn with_many_runs<Fut>(
        do_run: impl Fn(&ConnectedWorkers, RunId, IdVec, IdVec),
        check: impl Fn(Vec<AnyError>, IdVec, IdVec) -> Fut + Copy,
        timeout: Duration,
    ) where
        Fut: Future<Output = ()>,
    {
        let workers = ConnectedWorkers::new(timeout);

        let run_ids: Vec<_> = (0..MULTIPLE_RUN_IDS).map(|_| RunId::unique()).collect();
        let mut launched_by_id: Vec<_> = (0..MULTIPLE_RUN_IDS).map(|_| IdVec::default()).collect();
        let mut stopped_by_id: Vec<_> = (0..MULTIPLE_RUN_IDS).map(|_| IdVec::default()).collect();

        for i in 0..MULTIPLE_RUN_IDS {
            let run_id = run_ids[i].clone();
            let launched = launched_by_id[i].clone();
            let stopped = stopped_by_id[i].clone();
            do_run(&workers, run_id, launched, stopped);
        }
        let mut all_test_futs = vec![];
        for i in 0..MULTIPLE_RUN_IDS {
            // take so that we can check for unique reference of the pointer.
            let launched = std::mem::take(&mut launched_by_id[i]);
            let stopped = std::mem::take(&mut stopped_by_id[i]);

            let run_id = run_ids[i].clone();
            let workers = &workers;
            let fut = async move {
                let errors = match workers.stop(&run_id).await {
                    StopResult::Stopped(errors) => errors,
                    _ => unreachable!(),
                };
                check(errors, launched, stopped).await;
            };
            all_test_futs.push(fut);
        }
        let completed = futures::future::join_all(all_test_futs).await;
        assert_eq!(completed.len(), MULTIPLE_RUN_IDS);
    }

    #[tokio::test]
    async fn create_and_stop_nothing_created() {
        let workers = ConnectedWorkers::default();
        let run_id = RunId::unique();
        let stopped = workers.stop(&run_id).await;
        assert!(matches!(stopped, StopResult::RunNotAssociated));
    }

    #[tokio::test]
    async fn create_and_stop_multiple() {
        with_many_runs(
            |workers, run_id, launched, stopped| {
                for i in 0..MULTIPLE_WORKERS {
                    workers.insert(run_id.clone(), |rx_stop| {
                        let launched = launched.clone();
                        let stopped = stopped.clone();
                        async move {
                            launched.lock().await.push(i);
                            rx_stop.await?;
                            stopped.lock().await.push(i);
                            Ok(())
                        }
                    });
                }
            },
            |errors, launched, stopped| async move {
                assert!(errors.is_empty());

                let (mut launched, mut stopped) = (launched.lock().await, stopped.lock().await);
                launched.sort_unstable();
                stopped.sort_unstable();
                assert_eq!(&*launched, &(0..MULTIPLE_WORKERS).collect::<Vec<_>>());
                assert_eq!(&*stopped, &(0..MULTIPLE_WORKERS).collect::<Vec<_>>());
            },
            Duration::MAX,
        )
        .await;
    }

    #[tokio::test]
    async fn stopped_before_rx_stop() {
        with_many_runs(
            |workers, run_id, launched, _stopped| {
                workers.insert(run_id, |_rx_stop| {
                    let launched = launched.clone();
                    async move {
                        launched.lock().await.push(1);
                        Ok(())
                    }
                });
            },
            |errors, launched, _stopped| async move {
                assert!(errors.is_empty());

                let launched = launched.lock().await;
                assert_eq!(&*launched, &[1]);
            },
            Duration::MAX,
        )
        .await;
    }

    #[tokio::test]
    async fn stopped_with_error_multiple() {
        with_many_runs(
            |workers, run_id, launched, _stopped| {
                for i in 0..MULTIPLE_WORKERS {
                    workers.insert(run_id.clone(), |rx_stop| {
                        let launched = launched.clone();
                        async move {
                            launched.lock().await.push(i);
                            rx_stop.await?;
                            Err(format!("{i}").into())
                        }
                    });
                }
            },
            |errors, launched, _stopped| async move {
                let mut errors: Vec<_> = errors
                    .into_iter()
                    .map(|e| e.to_string().parse::<usize>().unwrap())
                    .collect();
                errors.sort();

                assert_eq!(errors.len(), MULTIPLE_WORKERS);
                for (i, e) in errors.into_iter().enumerate() {
                    assert_eq!(e, i);
                }

                let mut launched = launched.lock().await;
                launched.sort_unstable();
                assert_eq!(&*launched, &(0..MULTIPLE_WORKERS).collect::<Vec<_>>());
            },
            Duration::MAX,
        )
        .await;
    }

    #[tokio::test]
    async fn stopped_with_error_before_rx_stop() {
        with_many_runs(
            |workers, run_id, launched, _stopped| {
                workers.insert(run_id, |_rx_stop| {
                    let launched = launched.clone();
                    async move {
                        launched.lock().await.push(1);
                        Err("i died".into())
                    }
                });
            },
            |errors, launched, _stopped| async move {
                let launched = launched.lock().await;
                assert_eq!(&*launched, &[1]);

                assert_eq!(errors.len(), 1);
                assert_eq!(errors[0].to_string(), "i died");
            },
            Duration::MAX,
        )
        .await;
    }

    #[tokio::test]
    async fn forced_abort_multiple() {
        with_many_runs(
            |workers, run_id, _launched, _stopped| {
                for _ in 0..MULTIPLE_WORKERS {
                    workers.insert(run_id.clone(), |_rx_stop| async move {
                        tokio::time::sleep(Duration::MAX).await;
                        Ok(())
                    });
                }
            },
            |errors, launched, _stopped| async move {
                assert_eq!(errors.len(), 1);
                assert!(errors[0].to_string().starts_with("Forced abort"));

                assert_eq!(
                    Arc::strong_count(&launched),
                    1,
                    "outstanding references to launched, not all tasks are dead"
                );
                assert_eq!(
                    Arc::weak_count(&launched),
                    0,
                    "someone took a weak reference of launched?"
                );
            },
            Duration::ZERO,
        )
        .await;
    }
}
