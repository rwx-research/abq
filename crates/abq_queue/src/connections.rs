//! Persisted connections between the queue and workers.

#![cfg(test)] // TODO #281

use std::{collections::HashMap, future::Future, time::Duration};

use abq_utils::net_protocol::workers::RunId;
use parking_lot::Mutex;
use tokio::{sync::oneshot, task::JoinSet};

use crate::prelude::{AnyError, OpaqueResult};

#[derive(Default)]
pub(crate) struct ConnectedWorkers {
    map: Mutex<
        //
        HashMap<RunId, PersistedConnectionSet>,
    >,
}

struct PersistedConnectionSet {
    timeout: Duration,
    // NB: we do not care about the ordering of launched tasks, so we prefer
    // provided collections here over ordered ones.
    all_tasks: JoinSet<OpaqueResult<()>>,
    all_tx_stop: Vec<oneshot::Sender<()>>,
}

const DEFAULT_STOP_TIMEOUT: Duration = Duration::from_secs(10);

impl ConnectedWorkers {
    /// Create a set of connected workers associated to a run.
    ///
    /// # Invariants
    ///
    /// - This must be called before [insert][Self::insert] of any associated worker connection.
    /// - No other association for the same test run already exists.
    ///
    /// These invariants should be guaranteed by the state machine the queue moves test runs
    /// through.
    ///
    /// Panics if any invariant is broken.
    pub fn create(&self, run_id: RunId) {
        self.create_with_timeout(run_id, DEFAULT_STOP_TIMEOUT)
    }

    fn create_with_timeout(&self, run_id: RunId, timeout: Duration) {
        let mut map = self.map.lock();
        assert!(
            !map.contains_key(&run_id),
            "existing worker connection pool for {run_id:?}"
        );
        map.insert(
            run_id,
            PersistedConnectionSet {
                timeout,
                all_tasks: JoinSet::new(),
                all_tx_stop: Vec::new(),
            },
        );
    }

    /// Launches a task responsible for communicating with a persisted worker connection for a run.
    /// This method will call `spawn` itself; you should not do so yourself.
    ///
    /// # Invariants
    ///
    /// - Must be called in an async (tokio) runtime
    /// - This must be called only after an association for a run has been [created][Self::create].
    pub fn insert<F, Fut>(&self, run_id: &RunId, create_task: F)
    where
        F: FnOnce(/* on stop */ oneshot::Receiver<()>) -> Fut,
        Fut: Future<Output = OpaqueResult<()>> + Send + 'static,
    {
        let (tx_stop, rx_stop) = oneshot::channel();
        let task = create_task(rx_stop);
        {
            let mut map = self.map.lock();
            let conn_set = map.get_mut(run_id).unwrap_or_else(|| {
                panic!("worker connection pool for {run_id:?} must be present before insertion")
            });

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
    /// forcably aborted.
    ///
    /// Returns errors discovered when stopping the tasks, if any.
    ///
    /// # Invariants
    ///
    /// - The test run association must already exist.
    pub async fn stop(&self, run_id: &RunId) -> Vec<AnyError> {
        let PersistedConnectionSet {
            timeout,
            mut all_tasks,
            all_tx_stop,
        } = self.map.lock().remove(run_id).unwrap_or_else(|| {
            panic!("worker connection pool for {run_id:?} must be present before stopping")
        });

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
                result
            }
            _ = tokio::time::sleep(timeout) => {
                tracing::error!(?run_id, ?timeout, "forcing abort for worker connections failing to exit");
                all_tasks.shutdown().await;
                vec!["Forced abort for worker connections".into()]
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{future::Future, sync::Arc, time::Duration};

    use abq_utils::net_protocol::workers::RunId;
    use tokio::sync::Mutex;

    use crate::prelude::AnyError;

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
        let workers = ConnectedWorkers::default();

        let run_ids: Vec<_> = (0..MULTIPLE_RUN_IDS).map(|_| RunId::unique()).collect();
        let mut launched_by_id: Vec<_> = (0..MULTIPLE_RUN_IDS).map(|_| IdVec::default()).collect();
        let mut stopped_by_id: Vec<_> = (0..MULTIPLE_RUN_IDS).map(|_| IdVec::default()).collect();

        for run_id in run_ids.iter().cloned() {
            workers.create_with_timeout(run_id, timeout);
        }
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
                let errors = workers.stop(&run_id).await;
                check(errors, launched, stopped).await;
            };
            all_test_futs.push(fut);
        }
        let completed = futures::future::join_all(all_test_futs).await;
        assert_eq!(completed.len(), MULTIPLE_RUN_IDS);
    }

    #[tokio::test]
    async fn create_and_stop_empty() {
        let workers = ConnectedWorkers::default();
        let run_id = RunId::unique();
        workers.create(run_id.clone());
        let errors = workers.stop(&run_id).await;
        assert!(errors.is_empty());
    }

    #[tokio::test]
    async fn create_and_stop_multiple() {
        with_many_runs(
            |workers, run_id, launched, stopped| {
                for i in 0..MULTIPLE_WORKERS {
                    workers.insert(&run_id, |rx_stop| {
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
                workers.insert(&run_id, |_rx_stop| {
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
                    workers.insert(&run_id, |rx_stop| {
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
                workers.insert(&run_id, |_rx_stop| {
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
                    workers.insert(&run_id, |_rx_stop| async move {
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
