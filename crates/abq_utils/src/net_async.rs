//! Async-runtime-compatible server/client interfaces for use among entities in the ABQ ecosystem.
//! Rely on the [tokio] runtime.

use std::{io, net::SocketAddr};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::auth::Role;

#[async_trait]
pub trait ServerListener: Sync + Send {
    fn local_addr(&self) -> io::Result<SocketAddr>;

    /// Accept a stream, without performing any additional validation.
    /// This is meant to be called in main paths to avoid blocking; to get a stream that you can
    /// actually communicate across, feed the result to [ServerHandshakeCtx::handshake].
    ///
    /// This method is cancel-safe. If the future is cancelled before yielding, no new connection
    /// was accepted.
    async fn accept(&self) -> io::Result<(UnverifiedServerStream, SocketAddr)>;

    fn handshake_ctx(&self) -> Box<dyn ServerHandshakeCtx>;
}

#[async_trait]
pub trait ServerHandshakeCtx: Send + Sync {
    /// Handshake an [`UnverifiedServerStream`] to yield a stream that be communicated across.
    ///
    /// It is expected that the passed unverified stream corresponds to this [ServerListener]'s
    /// returned stream from [ServerListener::accept]. It is an error to pass a stream from another
    /// server listener, and calls to this function will panic if this condition is broken.
    async fn handshake(
        &self,
        unverified: UnverifiedServerStream,
    ) -> io::Result<Box<dyn ServerStream>>;
}

/// A stream that has been accepted by a server but not yet verified, for whatever reason.
/// To get a concrete stream you can read and write across, feed this stream back into the
/// accepting [Server][ServerListener]'s [ServerHandshakeCtx::handshake]
pub struct UnverifiedServerStream(tokio::net::TcpStream);

pub trait ServerStream: std::fmt::Debug + AsyncRead + AsyncWrite + Send + Sync + Unpin {
    fn peer_addr(&self) -> io::Result<SocketAddr>;

    /// The [Role] of the connected client this stream represents.
    fn role(&self) -> Role;
}

#[async_trait]
pub trait ConfiguredClient: Send + Sync {
    async fn connect(&self, addr: SocketAddr) -> io::Result<Box<dyn ClientStream>>;
    fn boxed_clone(&self) -> Box<dyn ConfiguredClient>;
}

pub trait ClientStream: AsyncRead + AsyncWrite + Unpin + Send + Sync {
    fn local_addr(&self) -> io::Result<SocketAddr>;
}

pub mod tcp;
pub mod tls;
