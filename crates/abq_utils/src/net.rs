//! Server/client interfaces for use among entities in the ABQ ecosystem.

pub(crate) mod tcp;
#[cfg(not(feature = "tls"))]
pub type ServerListener = tcp::ServerListener;
#[cfg(not(feature = "tls"))]
pub type ServerStream = tcp::Stream;
#[cfg(not(feature = "tls"))]
pub type ConfiguredClient = tcp::ConfiguredClient;
#[cfg(not(feature = "tls"))]
pub type ClientStream = tcp::Stream;

#[cfg(feature = "tls")]
pub(crate) mod tls;
#[cfg(feature = "tls")]
pub type ServerListener = tls::ServerListener;
#[cfg(feature = "tls")]
pub type ServerStream = tls::ServerStream;
#[cfg(feature = "tls")]
pub type ConfiguredClient = tls::ConfiguredClient;
#[cfg(feature = "tls")]
pub type ClientStream = tls::ClientStream;
