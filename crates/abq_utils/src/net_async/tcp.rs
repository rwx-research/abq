//! Raw tokio-async TCP connections.
//! Must be created in a Tokio runtime.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::ToSocketAddrs;

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
}

impl ServerListener {
    pub async fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Ok(Self { listener })
    }

    pub fn from_sync(listener: crate::net::tcp::ServerListener) -> io::Result<Self> {
        let crate::net::tcp::ServerListener { listener } = listener;
        listener.set_nonblocking(true)?;
        let listener = tokio::net::TcpListener::from_std(listener)?;
        Ok(Self { listener })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn accept(&self) -> io::Result<(Stream, SocketAddr)> {
        let (stream, addr) = self.listener.accept().await?;
        Ok((Stream(stream), addr))
    }
}

pub struct ConfiguredClient;

impl ConfiguredClient {
    pub fn new() -> io::Result<Self> {
        Ok(ConfiguredClient)
    }

    pub async fn connect(&self, addr: SocketAddr) -> io::Result<Stream> {
        Stream::connect(addr).await
    }
}
