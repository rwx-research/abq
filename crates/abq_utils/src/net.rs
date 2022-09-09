//! Server/client interfaces for use among entities in the ABQ ecosystem.

use std::{
    io::{self, Read, Write},
    net::SocketAddr,
};

pub trait ServerListener: Send + Sync {
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()>;
    fn local_addr(&self) -> io::Result<SocketAddr>;
    fn accept(&self) -> io::Result<(Box<dyn ServerStream>, SocketAddr)>;
    fn into_async(self: Box<Self>) -> io::Result<Box<dyn crate::net_async::ServerListener>>;
}

pub trait ServerStream: Read + Write {
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()>;
}

pub trait ConfiguredClient: Send + Sync {
    fn connect(&self, addr: SocketAddr) -> io::Result<Box<dyn ClientStream>>;
    fn boxed_clone(&self) -> Box<dyn ConfiguredClient>;
}

pub trait ClientStream: Read + Write {}

pub mod tcp;
pub mod tls;
