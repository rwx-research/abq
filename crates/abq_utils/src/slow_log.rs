//! Helper for logging when an awaited operation is taking unusually long, without forcing a
//! timeout. Useful as a diagnostic breadcrumb for hangs we want to observe rather than paper over.

use std::future::Future;
use std::time::Duration;

use tokio::pin;

/// Awaits `fut` to completion. If it has not completed after `threshold`, emits a
/// `warn!`-level log with `label` plus `threshold_secs`, and continues waiting. Logs at
/// most once per call; does not force a timeout.
///
/// Intended for diagnosing hangs at network/channel call sites that are otherwise unbounded.
pub async fn log_if_slow<F: Future>(label: &'static str, threshold: Duration, fut: F) -> F::Output {
    pin!(fut);
    tokio::select! {
        v = &mut fut => return v,
        _ = tokio::time::sleep(threshold) => {
            tracing::warn!(
                label,
                threshold_secs = threshold.as_secs(),
                "operation still pending past threshold"
            );
        }
    }
    fut.await
}
