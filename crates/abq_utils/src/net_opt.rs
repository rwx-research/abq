use std::{io, net::ToSocketAddrs};

use crate::{
    auth::{ClientAuthStrategy, ServerAuthStrategy},
    net, net_async,
};

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct Tls(bool);

impl Tls {
    pub const YES: Self = Tls(true);
    pub const NO: Self = Tls(false);
}

impl From<bool> for Tls {
    fn from(tls: bool) -> Self {
        Self(tls)
    }
}

impl std::str::FromStr for Tls {
    type Err = std::str::ParseBoolError;

    fn from_str(tls: &str) -> Result<Self, Self::Err> {
        bool::from_str(tls).map(Self)
    }
}

#[derive(Clone, Copy)]
pub struct ServerOptions {
    auth_strategy: ServerAuthStrategy,
    tls: Tls,
}

impl ServerOptions {
    pub fn new(auth_strategy: ServerAuthStrategy, tls: Tls) -> Self {
        Self { auth_strategy, tls }
    }

    /// Builds a new [ServerListener], ready to accept new connections.
    pub fn bind(self, addr: impl ToSocketAddrs) -> io::Result<Box<dyn net::ServerListener>> {
        let Self { tls, auth_strategy } = self;
        let client: Box<dyn net::ServerListener> = match tls {
            Tls::YES => net::tls::ServerListener::bind(auth_strategy, addr).map(Box::new)?,
            Tls::NO => net::tcp::ServerListener::bind(auth_strategy, addr).map(Box::new)?,
        };
        Ok(client)
    }

    pub async fn bind_async(
        self,
        addr: impl tokio::net::ToSocketAddrs,
    ) -> io::Result<Box<dyn net_async::ServerListener>> {
        let Self { tls, auth_strategy } = self;
        let client: Box<dyn net_async::ServerListener> = match tls {
            Tls::YES => net_async::tls::ServerListener::bind(auth_strategy, addr)
                .await
                .map(Box::new)?,
            Tls::NO => net_async::tcp::ServerListener::bind(auth_strategy, addr)
                .await
                .map(Box::new)?,
        };
        Ok(client)
    }
}

#[derive(Clone, Copy)]
pub struct ClientOptions {
    auth_strategy: ClientAuthStrategy,
    tls: Tls,
}

impl ClientOptions {
    pub fn new(auth_strategy: ClientAuthStrategy, tls: Tls) -> Self {
        Self { auth_strategy, tls }
    }

    /// Builds a new [ConfiguredClient], ready to issue new connections.
    pub fn build(self) -> io::Result<Box<dyn net::ConfiguredClient>> {
        let Self { tls, auth_strategy } = self;
        let client: Box<dyn net::ConfiguredClient> = match tls {
            Tls::YES => net::tls::ConfiguredClient::new(auth_strategy).map(Box::new)?,
            Tls::NO => net::tcp::ConfiguredClient::new(auth_strategy).map(Box::new)?,
        };
        Ok(client)
    }

    pub fn build_async(self) -> io::Result<Box<dyn net_async::ConfiguredClient>> {
        let Self { tls, auth_strategy } = self;
        let client: Box<dyn net_async::ConfiguredClient> = match tls {
            Tls::YES => net_async::tls::ConfiguredClient::new(auth_strategy).map(Box::new)?,
            Tls::NO => net_async::tcp::ConfiguredClient::new(auth_strategy).map(Box::new)?,
        };
        Ok(client)
    }
}
