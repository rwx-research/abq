use std::{future::Future, time::Duration};

/// A wrapper around a future that implements timing-out of that future.
pub struct TimeoutFuture<F: Future> {
    fut: F,
    timeout: Duration,
}

impl<F: Future> TimeoutFuture<F> {
    pub fn new(fut: F, timeout: Duration) -> Self {
        Self { fut, timeout }
    }

    /// Waits for the future to complete. Returns [None] if it times out instead.
    ///
    /// **Not** cancel-safe.
    pub async fn wait(self) -> Option<F::Output> {
        tokio::select! {
            r = self.fut => {
                Some(r)
            }
            _ = tokio::time::sleep(self.timeout) => {
                None
            }
        }
    }
}
