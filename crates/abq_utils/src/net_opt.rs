use std::{io, net::ToSocketAddrs};

use crate::{
    auth::{ClientAuthStrategy, ServerAuthStrategy},
    net, net_async,
    tls::{ClientTlsStrategy, ServerTlsStrategy},
};

#[derive(Clone)]
pub struct ServerOptions {
    auth_strategy: ServerAuthStrategy,
    tls_strategy: ServerTlsStrategy,
}

impl ServerOptions {
    pub fn new(auth_strategy: ServerAuthStrategy, tls_strategy: ServerTlsStrategy) -> Self {
        Self {
            auth_strategy,
            tls_strategy,
        }
    }

    /// Builds a new [ServerListener], ready to accept new connections.
    pub fn bind(self, addr: impl ToSocketAddrs) -> io::Result<Box<dyn net::ServerListener>> {
        let Self {
            auth_strategy,
            tls_strategy,
        } = self;

        use crate::tls::ServerTlsStrategyInner::*;

        let client: Box<dyn net::ServerListener> = match tls_strategy.0 {
            Config(tls_config) => {
                net::tls::ServerListener::bind(auth_strategy, tls_config, addr).map(Box::new)?
            }
            NoTls => net::tcp::ServerListener::bind(auth_strategy, addr).map(Box::new)?,
        };
        Ok(client)
    }

    pub async fn bind_async(
        self,
        addr: impl tokio::net::ToSocketAddrs,
    ) -> io::Result<Box<dyn net_async::ServerListener>> {
        let Self {
            auth_strategy,
            tls_strategy,
        } = self;

        use crate::tls::ServerTlsStrategyInner::*;

        let client: Box<dyn net_async::ServerListener> = match tls_strategy.0 {
            Config(tls_config) => {
                net_async::tls::ServerListener::bind(auth_strategy, tls_config, addr)
                    .await
                    .map(Box::new)?
            }
            NoTls => net_async::tcp::ServerListener::bind(auth_strategy, addr)
                .await
                .map(Box::new)?,
        };
        Ok(client)
    }
}

#[derive(Clone)]
pub struct ClientOptions<Role> {
    auth_strategy: ClientAuthStrategy<Role>,
    tls_strategy: ClientTlsStrategy,
}

impl<Role> ClientOptions<Role>
where
    Role: Send + Sync + Copy + 'static,
{
    pub fn new(auth_strategy: ClientAuthStrategy<Role>, tls_strategy: ClientTlsStrategy) -> Self {
        Self {
            auth_strategy,
            tls_strategy,
        }
    }

    /// Builds a new [ConfiguredClient], ready to issue new connections.
    pub fn build(self) -> io::Result<Box<dyn net::ConfiguredClient>> {
        let Self {
            auth_strategy,
            tls_strategy,
        } = self;

        use crate::tls::ClientTlsStrategyInner::*;

        let client: Box<dyn net::ConfiguredClient> = match tls_strategy.0 {
            Config(tls_config) => {
                Box::new(net::tls::ConfiguredClient::new(auth_strategy, tls_config))
            }
            NoTls => net::tcp::ConfiguredClient::new(auth_strategy).map(Box::new)?,
        };
        Ok(client)
    }

    pub fn build_async(self) -> io::Result<Box<dyn net_async::ConfiguredClient>> {
        let Self {
            auth_strategy,
            tls_strategy,
        } = self;

        use crate::tls::ClientTlsStrategyInner::*;

        let client: Box<dyn net_async::ConfiguredClient> = match tls_strategy.0 {
            Config(tls_config) => {
                net_async::tls::ConfiguredClient::new(auth_strategy, tls_config).map(Box::new)?
            }
            NoTls => net_async::tcp::ConfiguredClient::new(auth_strategy).map(Box::new)?,
        };
        Ok(client)
    }
}
