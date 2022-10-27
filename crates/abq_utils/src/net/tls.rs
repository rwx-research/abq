//! TLS connections.

use std::io;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::sync::Arc;

use rustls as tls;
use tokio_rustls as tokio_tls;

use crate::auth::{ClientAuthStrategy, Role, ServerAuthStrategy};

#[derive(Debug)]
pub struct ServerStream(
    tls::StreamOwned<tls::ServerConnection, std::net::TcpStream>,
    Role,
);

impl Read for ServerStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for ServerStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl super::ServerStream for ServerStream {
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.0.sock.set_nonblocking(nonblocking)
    }

    fn role(&self) -> Role {
        self.1
    }
}

pub struct ServerListener {
    listener: TcpListener,
    tls_config: Arc<tls::ServerConfig>,
    auth_strategy: ServerAuthStrategy,
}

impl ServerListener {
    pub fn bind(auth_strategy: ServerAuthStrategy, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let tls_config = crate::tls::get_server_config()?;
        let listener = TcpListener::bind(addr)?;

        Ok(Self {
            listener,
            tls_config,
            auth_strategy,
        })
    }
}

impl super::ServerListener for ServerListener {
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.listener.set_nonblocking(nonblocking)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    fn accept(&self) -> io::Result<(Box<dyn super::ServerStream>, SocketAddr)> {
        let conn = tls::ServerConnection::new(Arc::clone(&self.tls_config))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let (sock, addr) = self.listener.accept()?;

        // Wrap the socket around the connection, moving us into speaking TLS
        let mut stream = tls::StreamOwned::new(conn, sock);

        // Before we give back the TLS stream, we must validate auth
        let role = self.auth_strategy.verify_header(&mut stream)?;

        Ok((Box::new(ServerStream(stream, role)), addr))
    }

    fn into_async(self: Box<Self>) -> io::Result<Box<dyn crate::net_async::ServerListener>> {
        let Self {
            listener,
            tls_config,
            auth_strategy,
        } = *self;
        listener.set_nonblocking(true)?;
        let listener = tokio::net::TcpListener::from_std(listener)?;

        let acceptor = tokio_tls::TlsAcceptor::from(tls_config);

        Ok(Box::new(crate::net_async::tls::ServerListener {
            listener,
            acceptor,
            auth_strategy,
        }))
    }
}

pub struct ClientStream(tls::StreamOwned<tls::ClientConnection, std::net::TcpStream>);

impl Read for ClientStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for ClientStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl super::ClientStream for ClientStream {}

#[derive(Clone)]
pub struct ConfiguredClient<Role> {
    tls_config: Arc<tls::ClientConfig>,
    auth_strategy: ClientAuthStrategy<Role>,
}

impl<Role> ConfiguredClient<Role> {
    pub fn new(auth_strategy: ClientAuthStrategy<Role>) -> io::Result<Self> {
        let tls_config = crate::tls::get_client_config()?;

        Ok(Self {
            tls_config,
            auth_strategy,
        })
    }
}

impl<Role> super::ConfiguredClient for ConfiguredClient<Role>
where
    Role: Send + Sync + Clone + 'static,
{
    fn connect(&self, addr: SocketAddr) -> io::Result<Box<dyn super::ClientStream>> {
        let server_name = crate::tls::get_server_name();

        let conn = tls::ClientConnection::new(Arc::clone(&self.tls_config), server_name)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let sock = std::net::TcpStream::connect(addr)?;

        // Wrap the socket around the connection, moving us into speaking TLS
        let stream = tls::StreamOwned::new(conn, sock);
        let mut stream = ClientStream(stream);

        // Before handing back the the stream, send over auth
        self.auth_strategy.send(&mut stream)?;

        Ok(Box::new(stream))
    }

    fn boxed_clone(&self) -> Box<dyn super::ConfiguredClient> {
        Box::new(self.clone())
    }
}
