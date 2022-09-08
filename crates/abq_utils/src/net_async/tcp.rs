//! Raw tokio-async TCP connections.
//! Must be created in a Tokio runtime.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::ToSocketAddrs;

use crate::auth::{ClientAuthStrategy, ServerAuthStrategy};

#[derive(Debug)]
pub struct Stream(tokio::net::TcpStream);

impl Stream {
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let stream = tokio::net::TcpStream::connect(addr).await?;
        Ok(Self(stream))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.local_addr()
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.0.peer_addr()
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        // TODO(ayaz): i think we can get rid of these pins we mark `Stream` as transparent, since
        // it should be compiled that way anyway.
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

pub struct ServerListener {
    listener: tokio::net::TcpListener,
    auth_strategy: ServerAuthStrategy,
}

impl ServerListener {
    pub async fn bind(
        auth_strategy: ServerAuthStrategy,
        addr: impl ToSocketAddrs,
    ) -> io::Result<Self> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            auth_strategy,
        })
    }

    pub fn from_sync(listener: crate::net::tcp::ServerListener) -> io::Result<Self> {
        let crate::net::tcp::ServerListener {
            listener,
            auth_strategy,
        } = listener;
        listener.set_nonblocking(true)?;
        let listener = tokio::net::TcpListener::from_std(listener)?;
        Ok(Self {
            listener,
            auth_strategy,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn accept(&self) -> io::Result<(Stream, SocketAddr)> {
        let (stream, addr) = self.listener.accept().await?;
        let mut stream = Stream(stream);

        // We need to validate any auth header before handing the connection over.
        self.auth_strategy.async_verify_header(&mut stream).await?;

        Ok((stream, addr))
    }
}

pub struct ConfiguredClient {
    auth_strategy: ClientAuthStrategy,
}

impl ConfiguredClient {
    pub fn new(auth_strategy: ClientAuthStrategy) -> io::Result<Self> {
        Ok(ConfiguredClient { auth_strategy })
    }

    pub async fn connect(&self, addr: SocketAddr) -> io::Result<Stream> {
        let mut stream = Stream::connect(addr).await?;

        // Make our auth header, if any, known to the server.
        self.auth_strategy.async_send(&mut stream).await?;

        Ok(stream)
    }
}
