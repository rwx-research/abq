//! Plain old raw TCP connections.
//!
//! A thin shell over the standard libraries' implementations.

use std::{
    io::{self, Read, Write},
    net::{SocketAddr, ToSocketAddrs},
};

use crate::auth::{ClientAuthStrategy, Role, ServerAuthStrategy};

pub struct ClientStream(std::net::TcpStream);

impl ClientStream {
    pub fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        std::net::TcpStream::connect(addr).map(Self)
    }
}

impl super::ClientStream for ClientStream {}

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

pub struct ServerStream(std::net::TcpStream, Role);

impl super::ServerStream for ServerStream {
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.0.set_nonblocking(nonblocking)
    }

    fn role(&self) -> crate::auth::Role {
        self.1
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
        let (mut stream, addr) = self.listener.accept()?;

        // Validate any auth before handing the connection back.
        let role = self.auth_strategy.verify_header(&mut stream)?;

        Ok((Box::new(ServerStream(stream, role)), addr))
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
pub struct ConfiguredClient<Role> {
    auth_strategy: ClientAuthStrategy<Role>,
}

impl<Role> ConfiguredClient<Role> {
    pub fn new(auth_strategy: ClientAuthStrategy<Role>) -> io::Result<Self> {
        Ok(ConfiguredClient { auth_strategy })
    }
}

impl<Role> super::ConfiguredClient for ConfiguredClient<Role>
where
    Role: Send + Sync + Copy + 'static,
{
    fn connect(&self, addr: SocketAddr) -> io::Result<Box<dyn super::ClientStream>> {
        let mut stream = ClientStream::connect(addr)?;

        // Send over auth before handing the stream back.
        self.auth_strategy.send(&mut stream)?;

        Ok(Box::new(stream))
    }

    fn boxed_clone(&self) -> Box<dyn super::ConfiguredClient> {
        Box::new(*self)
    }
}
