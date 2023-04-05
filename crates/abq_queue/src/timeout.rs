use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use abq_utils::net_protocol::workers::RunId;
use futures::{stream::FuturesUnordered, StreamExt};
use pin_project_lite::pin_project;
use std::future::Future;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct RunTimeoutStrategy(RunTimeoutStrategyPriv);

impl RunTimeoutStrategy {
    /// Determine the timeout from the parameters of a test run.
    pub const RUN_BASED: Self = RunTimeoutStrategy(RunTimeoutStrategyPriv::RunBased);

    /// Constant timeout, for use in testing.
    pub fn constant(f: fn(TimeoutReason) -> Duration) -> Self {
        Self(RunTimeoutStrategyPriv::Constant(f))
    }
}

#[derive(Debug, Clone, Copy)]
enum RunTimeoutStrategyPriv {
    RunBased,
    Constant(fn(TimeoutReason) -> Duration),
}

impl Default for RunTimeoutStrategy {
    fn default() -> Self {
        Self::RUN_BASED
    }
}

/// How long to wait before a manifest must make progress, or else its associated run is timed-out.
const WAIT_FOR_MANIFEST_PROGRESS_DURATION: Duration = Duration::from_secs(60 * 60); // 1 hour

impl RunTimeoutStrategy {
    /// Determine a duration for how long to wait before a manifest must make progress, or
    /// else its associated run is timed-out.
    pub(crate) fn wait_for_manifest_progress_duration(
        &self,
        current_manifest_index: usize,
    ) -> TimeoutSpec {
        let reason = TimeoutReason::WaitForManifestProgress {
            last_observed_test_index: current_manifest_index,
        };
        let duration = match self.0 {
            RunTimeoutStrategyPriv::RunBased => WAIT_FOR_MANIFEST_PROGRESS_DURATION,
            RunTimeoutStrategyPriv::Constant(timeout) => timeout(reason),
        };
        TimeoutSpec { duration, reason }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct TimeoutSpec {
    duration: Duration,
    reason: TimeoutReason,
}

struct RunTimeoutManagerInner {
    strategy: RunTimeoutStrategy,
    timeouts: RwLock<FuturesUnordered<TimeoutCell>>,
}

/// Records runs that should timeout after a certain amount of time.
/// Fires [RunTimeoutManager::next_timeout] when a timeout is reached.
///
/// Clones of the manager are cheap.
#[derive(Clone)]
pub(crate) struct RunTimeoutManager(Arc<RunTimeoutManagerInner>);

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TimeoutReason {
    /// A test run was timed-out because no progress was made in popping tests off the manifest in
    /// time.
    WaitForManifestProgress {
        /// Where in the manifest we were when the timeout was started.
        last_observed_test_index: usize,
    },
}

#[derive(Debug, PartialEq)]
pub(crate) struct FiredTimeout {
    pub run_id: RunId,
    pub after: Duration,
    pub reason: TimeoutReason,
}

pin_project! {
    struct TimeoutCell {
        #[pin]
        sleep: tokio::time::Sleep,
        fired_timeout: Option<FiredTimeout>,
    }
}

impl Future for TimeoutCell {
    type Output = FiredTimeout;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.project();
        match me.sleep.poll(cx) {
            Poll::Ready(()) => Poll::Ready(
                me.fired_timeout
                    .take()
                    .expect("ready futures are never re-polled."),
            ),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Default for RunTimeoutManager {
    fn default() -> Self {
        Self::new(RunTimeoutStrategy::default())
    }
}

impl RunTimeoutManager {
    const MAX_TIMEOUT_WAIT_TIME: Duration = Duration::from_micros(10);

    pub fn new(strategy: RunTimeoutStrategy) -> Self {
        Self(Arc::new(RunTimeoutManagerInner {
            strategy,
            timeouts: Default::default(),
        }))
    }

    pub fn strategy(&self) -> RunTimeoutStrategy {
        self.0.strategy
    }

    /// Inserts a new timeout.
    /// Thread safe and takes only a read over a read/write lock; this makes insertions relatively
    /// cheap across threads, while waiting for timeouts requires exclusive access.
    pub async fn insert(&self, run_id: RunId, timeout_spec: TimeoutSpec) {
        let TimeoutSpec { duration, reason } = timeout_spec;
        let runs = self.0.timeouts.read().await;
        let timeout_cell = TimeoutCell {
            sleep: tokio::time::sleep(duration),
            fired_timeout: Some(FiredTimeout {
                run_id,
                after: duration,
                reason,
            }),
        };

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
    pub async fn next_timeout(&self) -> FiredTimeout {
        loop {
            let mut runs = self.0.timeouts.write().await;
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

    use crate::timeout::{FiredTimeout, RunTimeoutStrategy, TimeoutReason, TimeoutSpec};

    use super::RunTimeoutManager;

    fn zero(_: TimeoutReason) -> Duration {
        Duration::ZERO
    }

    #[tokio::test]
    async fn fire_one_timeout() {
        let manager = RunTimeoutManager::new(RunTimeoutStrategy::constant(zero));

        let run_id = RunId::unique();

        let spec = manager.strategy().wait_for_manifest_progress_duration(0);

        manager.insert(run_id.clone(), spec).await;
        assert_eq!(
            manager.next_timeout().await,
            FiredTimeout {
                run_id,
                after: Duration::ZERO,
                reason: TimeoutReason::WaitForManifestProgress {
                    last_observed_test_index: 0
                },
            }
        );
    }

    #[tokio::test]
    async fn fire_multiple_timeouts() {
        let manager = RunTimeoutManager::new(RunTimeoutStrategy::constant(zero));

        let run_id1 = RunId::unique();
        let run_id2 = RunId::unique();
        let run_id3 = RunId::unique();

        let spec = TimeoutSpec {
            duration: Duration::ZERO,
            reason: TimeoutReason::WaitForManifestProgress {
                last_observed_test_index: 0,
            },
        };

        manager.insert(run_id1.clone(), spec).await;
        manager.insert(run_id2.clone(), spec).await;
        manager.insert(run_id3.clone(), spec).await;

        let mut timed_out = HashSet::new();
        let mut afters = HashSet::new();

        for _ in 0..3 {
            let FiredTimeout {
                run_id,
                after,
                reason: _,
            } = manager.next_timeout().await;
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
        let manager = RunTimeoutManager::new(RunTimeoutStrategy::default());

        // Make sure long-lived timeouts allow contest - `old` timeout here should allow `young` to
        // compete, and moreover `young` should always finish first.

        let old_timeout = Duration::MAX;
        let old_run_id = RunId::unique();

        let old_spec = TimeoutSpec {
            duration: old_timeout,
            reason: TimeoutReason::WaitForManifestProgress {
                last_observed_test_index: 0,
            },
        };

        let young_timeout = Duration::from_micros(10);
        let young_run_id = RunId::unique();

        let young_spec = TimeoutSpec {
            duration: young_timeout,
            reason: TimeoutReason::WaitForManifestProgress {
                last_observed_test_index: 0,
            },
        };

        manager.insert(old_run_id, old_spec).await;

        tokio::select! {
            _insert_young = manager.insert(young_run_id.clone(), young_spec) => (),
            _wait_for_old = manager.next_timeout() => unreachable!(),
        };

        assert_eq!(
            manager.next_timeout().await,
            FiredTimeout {
                run_id: young_run_id,
                after: young_timeout,
                reason: TimeoutReason::WaitForManifestProgress {
                    last_observed_test_index: 0
                },
            }
        )
    }
}
