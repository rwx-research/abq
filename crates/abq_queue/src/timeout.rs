#![allow(dead_code)] // TODO(#185) remove
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use abq_utils::net_protocol::workers::RunId;
use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use std::future::Future;
use tokio::sync::RwLock;

/// Records runs that should timeout after a certain amount of time.
/// Fires [RunTimeoutManager::next_timeout] when a timeout is reached.
#[derive(Default)]
pub(crate) struct RunTimeoutManager {
    timeouts: Arc<RwLock<FuturesUnordered<TimeoutCell>>>,
}

#[derive(Debug, PartialEq)]
pub(crate) struct TimedOutRun {
    run_id: RunId,
    after: Duration,
}

struct TimeoutCell {
    timeout: BoxFuture<'static, TimedOutRun>,
}

impl Future for TimeoutCell {
    type Output = TimedOutRun;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.timeout).poll(cx)
    }
}

impl RunTimeoutManager {
    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

    const MAX_TIMEOUT_WAIT_TIME: Duration = Duration::from_micros(10);

    /// Inserts a new timeout for a given run ID.
    /// Thread safe and takes only a read over a read/write lock; this makes insertions relatively
    /// cheap across threads, while waiting for timeouts requires exclusive access.
    pub async fn insert_run(&self, run_id: RunId, timeout: Duration) {
        let runs = self.timeouts.read().await;
        let fut = Box::pin(async move {
            tokio::time::sleep(timeout).await;
            TimedOutRun {
                run_id,
                after: timeout,
            }
        });
        let timeout_cell = TimeoutCell { timeout: fut };

        // NB: We should attempt to minimize memory leaks by dropping timeout
        // cells that are no longer active, or reusing their memory. The latter
        // is preferable, but since timeout allocations should be far and few-between
        // in a queue's lifetime, either is acceptable. The present implementation
        // of `FuturesUnordered` provides the former:
        //
        //   https://docs.rs/futures-util/0.3.25/src/futures_util/stream/futures_unordered/mod.rs.html#79-102
        runs.push(timeout_cell);
    }

    /// Yields the next timeout, once it fires. The procedure takes a write lock,
    /// but will yield at regular intervals for [new runs][Self::insert_run].
    /// If there are no timeouts, this call will not return, or will only return after a new
    /// timeout is inserted.
    pub async fn next_timeout(&self) -> TimedOutRun {
        loop {
            let mut runs = self.timeouts.write().await;
            // TODO: consider using futures::select_biased, since the max-wait path
            // is far more likely to fire.
            tokio::select! {
                _ = tokio::time::sleep(Self::MAX_TIMEOUT_WAIT_TIME) => {
                    continue;
                }
                Some(timeout) = runs.next() => {
                    return timeout;
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, time::Duration};

    use abq_utils::net_protocol::workers::RunId;

    use crate::timeout::TimedOutRun;

    use super::RunTimeoutManager;

    #[tokio::test]
    async fn fire_one_timeout() {
        let manager = RunTimeoutManager::default();

        let run_id = RunId::unique();

        manager.insert_run(run_id.clone(), Duration::ZERO).await;
        assert_eq!(
            manager.next_timeout().await,
            TimedOutRun {
                run_id,
                after: Duration::ZERO
            }
        );
    }

    #[tokio::test]
    async fn fire_multiple_timeouts() {
        let manager = RunTimeoutManager::default();

        let run_id1 = RunId::unique();
        let run_id2 = RunId::unique();
        let run_id3 = RunId::unique();

        manager.insert_run(run_id1.clone(), Duration::ZERO).await;
        manager.insert_run(run_id2.clone(), Duration::ZERO).await;
        manager.insert_run(run_id3.clone(), Duration::ZERO).await;

        let mut timed_out = HashSet::new();
        let mut afters = HashSet::new();

        for _ in 0..3 {
            let TimedOutRun { run_id, after } = manager.next_timeout().await;
            timed_out.insert(run_id);
            afters.insert(after);
        }

        assert_eq!(timed_out.len(), 3);
        assert!(timed_out.contains(&run_id1));
        assert!(timed_out.contains(&run_id2));
        assert!(timed_out.contains(&run_id3));

        assert_eq!(afters.len(), 1);
        assert!(afters.contains(&Duration::ZERO));
    }

    #[tokio::test]
    async fn yield_to_contest() {
        let manager = RunTimeoutManager::default();

        // Make sure long-lived timeouts allow contest - `old` timeout here should allow `young` to
        // compete, and moreover `young` should always finish first.

        let old_timeout = Duration::MAX;
        let old_run_id = RunId::unique();

        let young_timeout = Duration::from_micros(10);
        let young_run_id = RunId::unique();

        manager.insert_run(old_run_id, old_timeout).await;

        tokio::select! {
            _insert_young = manager.insert_run(young_run_id.clone(), young_timeout) => (),
            _wait_for_old = manager.next_timeout() => unreachable!(),
        };

        assert_eq!(
            manager.next_timeout().await,
            TimedOutRun {
                run_id: young_run_id,
                after: young_timeout,
            }
        )
    }
}