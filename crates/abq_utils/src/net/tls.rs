//! TLS connections.

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, ToSocketAddrs};
use std::sync::Arc;
use std::{fs, io, thread};

use rustls as tls;

use crate::auth::{ClientAuthStrategy, ServerAuthStrategy};

#[derive(Debug)]
pub struct ServerStream(tls::StreamOwned<tls::ServerConnection, std::net::TcpStream>);

impl ServerStream {
    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.0.sock.set_nonblocking(nonblocking)
    }
}

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

pub struct ServerListener {
    pub(crate) listener: TcpListener,
    pub(crate) tls_config: Arc<tls::ServerConfig>,
    pub(crate) auth_strategy: ServerAuthStrategy,
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

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.listener.set_nonblocking(nonblocking)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn accept(&self) -> io::Result<(ServerStream, SocketAddr)> {
        let conn = tls::ServerConnection::new(Arc::clone(&self.tls_config))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let (sock, addr) = self.listener.accept()?;

        // Wrap the socket around the connection, moving us into speaking TLS
        let stream = tls::StreamOwned::new(conn, sock);
        let mut stream = ServerStream(stream);

        // Before we give back the TLS stream, we must validate auth
        self.auth_strategy.verify_header(&mut stream)?;

        Ok((stream, addr))
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

#[derive(Clone)]
pub struct ConfiguredClient {
    tls_config: Arc<tls::ClientConfig>,
    auth_strategy: ClientAuthStrategy,
}

impl ConfiguredClient {
    pub fn new(auth_strategy: ClientAuthStrategy) -> io::Result<Self> {
        let tls_config = crate::tls::get_client_config()?;

        Ok(Self {
            tls_config,
            auth_strategy,
        })
    }

    pub fn connect(&self, addr: SocketAddr) -> io::Result<ClientStream> {
        let server_name = crate::tls::get_server_name();

        let conn = tls::ClientConnection::new(Arc::clone(&self.tls_config), server_name)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let sock = std::net::TcpStream::connect(addr)?;

        // Wrap the socket around the connection, moving us into speaking TLS
        let stream = tls::StreamOwned::new(conn, sock);
        let mut stream = ClientStream(stream);

        // Before handing back the the stream, send over auth
        self.auth_strategy.send(&mut stream)?;

        Ok(stream)
    }
}
