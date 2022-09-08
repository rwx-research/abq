//! Async-runtime-compatible server/client interfaces for use among entities in the ABQ ecosystem.
//! Rely on the [tokio] runtime.

mod tcp;

pub type ServerListener = tcp::ServerListener;
pub type ServerStream = tcp::Stream;
pub type ConfiguredClient = tcp::ConfiguredClient;
pub type ClientStream = tcp::Stream;
