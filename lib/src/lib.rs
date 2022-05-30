#![cfg(unix)] //  Only targeting unix for now because the Queue exclusively uses unix sockets.

pub mod notify;
pub mod protocol;
pub mod queue;
mod workers;

#[cfg(test)]
procspawn::enable_test_support!();
