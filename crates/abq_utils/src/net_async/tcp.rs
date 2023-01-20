//! Raw tokio-async TCP connections.
//! Must be created in a Tokio runtime.

use async_trait::async_trait;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::ToSocketAddrs;

use super::UnverifiedServerStream;
use crate::auth::{ClientAuthStrategy, Role, ServerAuthStrategy};

pub struct RawServerStream(tokio::net::TcpStream);

#[derive(Debug)]
#[repr(transparent)]
pub struct ClientStream(tokio::net::TcpStream);

impl ClientStream {
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let stream = tokio::net::TcpStream::connect(addr).await?;
        Ok(Self(stream))
    }
}

impl super::ClientStream for ClientStream {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.local_addr()
    }
}

impl AsyncRead for ClientStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for ClientStream {
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

#[derive(Debug)]
pub struct ServerStream(tokio::net::TcpStream, Role);

impl super::ServerStream for ServerStream {
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.0.peer_addr()
    }

    fn role(&self) -> Role {
        self.1
    }
}

impl AsyncRead for ServerStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for ServerStream {
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
    pub(crate) listener: tokio::net::TcpListener,
    pub(crate) auth_strategy: ServerAuthStrategy,
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
}

struct HandshakeCtx {
    auth_strategy: ServerAuthStrategy,
}

#[async_trait]
impl super::ServerListener for ServerListener {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    async fn accept(&self) -> io::Result<(UnverifiedServerStream, SocketAddr)> {
        let (stream, addr) = self.listener.accept().await?;
        Ok((UnverifiedServerStream(stream), addr))
    }

    fn handshake_ctx(&self) -> Box<dyn super::ServerHandshakeCtx> {
        Box::new(HandshakeCtx {
            auth_strategy: self.auth_strategy,
        })
    }
}

#[async_trait]
impl super::ServerHandshakeCtx for HandshakeCtx {
    async fn handshake(
        &self,
        unverified: UnverifiedServerStream,
    ) -> io::Result<Box<dyn super::ServerStream>> {
        let UnverifiedServerStream(mut stream) = unverified;

        // We need to validate any auth header before handing the connection over.
        let role = self.auth_strategy.async_verify_header(&mut stream).await?;

        Ok(Box::new(ServerStream(stream, role)))
    }
}

pub struct ConfiguredClient<Role> {
    auth_strategy: ClientAuthStrategy<Role>,
}

impl<Role> ConfiguredClient<Role> {
    pub fn new(auth_strategy: ClientAuthStrategy<Role>) -> io::Result<Self> {
        Ok(ConfiguredClient { auth_strategy })
    }
}

#[async_trait]
impl<Role> super::ConfiguredClient for ConfiguredClient<Role>
where
    Role: Send + Sync + Clone + 'static,
{
    async fn connect(&self, addr: SocketAddr) -> io::Result<Box<dyn super::ClientStream>> {
        let mut stream = ClientStream::connect(addr).await?;

        // Make our auth header, if any, known to the server.
        self.auth_strategy.async_send(&mut stream).await?;

        Ok(Box::new(stream))
    }

    fn boxed_clone(&self) -> Box<dyn super::ConfiguredClient> {
        Box::new(Self {
            auth_strategy: self.auth_strategy.clone(),
        })
    }
}
