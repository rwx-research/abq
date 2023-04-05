#![cfg(unix)] //  Only targeting unix for now because the Queue exclusively uses unix sockets.

mod prelude;

mod job_queue;
pub mod persistence;
pub mod queue;
mod timeout;
mod worker_timings;
mod worker_tracking;

pub use timeout::{RunTimeoutStrategy, TimeoutReason};
