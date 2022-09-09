//! Async-runtime-compatible server/client interfaces for use among entities in the ABQ ecosystem.
//! Rely on the [tokio] runtime.

use std::{io, net::SocketAddr};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

#[async_trait]
pub trait ServerListener {
    fn local_addr(&self) -> io::Result<SocketAddr>;
    async fn accept(&self) -> io::Result<(Box<dyn ServerStream>, SocketAddr)>;
}

pub trait ServerStream: std::fmt::Debug + AsyncRead + AsyncWrite + Send + Sync + Unpin {
    fn peer_addr(&self) -> io::Result<SocketAddr>;
}

#[async_trait]
pub trait ConfiguredClient {
    async fn connect(&self, addr: SocketAddr) -> io::Result<Box<dyn ClientStream>>;
}

pub trait ClientStream: AsyncRead + AsyncWrite + Unpin {
    fn local_addr(&self) -> io::Result<SocketAddr>;
}

pub mod tcp;
pub mod tls;
