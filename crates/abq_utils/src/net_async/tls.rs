//! TLS-based connections.
//! Must be created in a Tokio runtime.

use async_trait::async_trait;
use rustls as tls;
use tokio_rustls as tokio_tls;

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::ToSocketAddrs;

use super::UnverifiedServerStream;
use crate::auth::{ClientAuthStrategy, Role, ServerAuthStrategy};

#[derive(Debug)]
pub struct ServerStream(tokio_tls::server::TlsStream<tokio::net::TcpStream>, Role);

impl super::ServerStream for ServerStream {
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.0.get_ref().0.peer_addr()
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
    pub(crate) acceptor: tokio_tls::TlsAcceptor,
    pub(crate) auth_strategy: ServerAuthStrategy,
}

impl ServerListener {
    pub async fn bind(
        auth_strategy: ServerAuthStrategy,
        tls_config: Arc<tls::ServerConfig>,
        addr: impl ToSocketAddrs,
    ) -> io::Result<Self> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let acceptor = tokio_tls::TlsAcceptor::from(tls_config);

        Ok(Self {
            listener,
            acceptor,
            auth_strategy,
        })
    }
}

struct HandshakeCtx {
    acceptor: tokio_tls::TlsAcceptor,
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
            acceptor: self.acceptor.clone(),
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
        let UnverifiedServerStream(stream) = unverified;

        let mut stream = self.acceptor.accept(stream).await?;

        // We now have a stream talking TLS, and we need to validate any auth header
        // before handing the connection over.
        let role = self.auth_strategy.async_verify_header(&mut stream).await?;

        Ok(Box::new(ServerStream(stream, role)))
    }
}

#[repr(transparent)]
pub struct ClientStream(tokio_tls::client::TlsStream<tokio::net::TcpStream>);

impl super::ClientStream for ClientStream {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.get_ref().0.local_addr()
    }
}

impl AsyncRead for ClientStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for ClientStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

pub struct ConfiguredClient<Role> {
    connector: tokio_tls::TlsConnector,
    auth_strategy: ClientAuthStrategy<Role>,
}

impl<Role> ConfiguredClient<Role> {
    pub fn new(
        auth_strategy: ClientAuthStrategy<Role>,
        tls_config: Arc<tls::ClientConfig>,
    ) -> io::Result<Self> {
        let connector = tokio_tls::TlsConnector::from(tls_config);

        Ok(Self {
            connector,
            auth_strategy,
        })
    }
}

#[async_trait]
impl<Role> super::ConfiguredClient for ConfiguredClient<Role>
where
    Role: Send + Sync + Clone + 'static,
{
    async fn connect(&self, addr: SocketAddr) -> io::Result<Box<dyn super::ClientStream>> {
        let sock = tokio::net::TcpStream::connect(addr).await?;

        // NB: we are assuming we are only ever connecting to an ABQ server!
        let dns_name = crate::tls::get_server_name();

        let stream = self.connector.connect(dns_name, sock).await?;
        let mut stream = ClientStream(stream);

        // Send any auth header over immediately.
        self.auth_strategy.async_send(&mut stream).await?;

        Ok(Box::new(stream))
    }

    fn boxed_clone(&self) -> Box<dyn super::ConfiguredClient> {
        Box::new(Self {
            connector: self.connector.clone(),
            auth_strategy: self.auth_strategy.clone(),
        })
    }
}
