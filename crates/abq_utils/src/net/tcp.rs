//! Plain old raw TCP connections.
//!
//! A thin shell over the standard libraries' implementations.

use std::{
    io::{self, Read, Write},
    net::{SocketAddr, ToSocketAddrs},
};

use crate::auth::{ClientAuthStrategy, ServerAuthStrategy};

pub struct Stream(std::net::TcpStream);

impl Stream {
    pub fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        std::net::TcpStream::connect(addr).map(Self)
    }
}

impl super::ServerStream for Stream {
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.0.set_nonblocking(nonblocking)
    }
}

impl super::ClientStream for Stream {}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

pub struct ServerListener {
    listener: std::net::TcpListener,
    auth_strategy: ServerAuthStrategy,
}

impl ServerListener {
    pub fn bind(auth_strategy: ServerAuthStrategy, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let listener = std::net::TcpListener::bind(addr)?;
        Ok(Self {
            listener,
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
        let (stream, addr) = self.listener.accept()?;
        let mut stream = Stream(stream);

        // Validate any auth before handing the connection back.
        self.auth_strategy.verify_header(&mut stream)?;

        Ok((Box::new(stream), addr))
    }

    fn into_async(self: Box<Self>) -> io::Result<Box<dyn crate::net_async::ServerListener>> {
        let Self {
            listener,
            auth_strategy,
        } = *self;
        listener.set_nonblocking(true)?;
        let listener = tokio::net::TcpListener::from_std(listener)?;
        Ok(Box::new(crate::net_async::tcp::ServerListener {
            listener,
            auth_strategy,
        }))
    }
}

#[derive(Clone, Copy)]
pub struct ConfiguredClient {
    auth_strategy: ClientAuthStrategy,
}

impl ConfiguredClient {
    pub fn new(auth_strategy: ClientAuthStrategy) -> io::Result<Self> {
        Ok(ConfiguredClient { auth_strategy })
    }
}

impl super::ConfiguredClient for ConfiguredClient {
    fn connect(&self, addr: SocketAddr) -> io::Result<Box<dyn super::ClientStream>> {
        let mut stream = Stream::connect(addr)?;

        // Send over auth before handing the stream back.
        self.auth_strategy.send(&mut stream)?;

        Ok(Box::new(stream))
    }

    fn boxed_clone(&self) -> Box<dyn super::ConfiguredClient> {
        Box::new(*self)
    }
}
