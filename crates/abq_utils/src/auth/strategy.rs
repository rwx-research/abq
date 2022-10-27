use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::auth::{token::TOKEN_LEN, ClientToken};

use std::io::{self, Read, Write};

use super::token::Token;

pub fn build_strategies(auth_token: ClientToken) -> (ServerAuthStrategy, ClientAuthStrategy) {
    (
        ServerAuthStrategy::Token(auth_token),
        ClientAuthStrategy::Token(auth_token),
    )
}

#[derive(Clone, Copy)]
pub enum ClientAuthStrategy {
    /// The client sends no auth, and expects the server not to check for any.
    NoAuth,
    /// The client sends a constant [AuthToken] to the server.
    Token(ClientToken),
}

impl ClientAuthStrategy {
    /// Attaches auth as a constant header to a writable stream, intended to be a connection to a
    /// server.
    /// This should be the first write done in a connection to a server.
    pub fn send(&self, stream: &mut impl Write) -> io::Result<()> {
        match self {
            ClientAuthStrategy::NoAuth => {
                // noop
            }
            ClientAuthStrategy::Token(token) => {
                // Unlike the standard network protocol, we write auth tokens as a fixed-size
                // header.
                stream.write_all(token.raw_bytes())?;
            }
        }
        Ok(())
    }

    /// Like [Self::send], but async.
    pub async fn async_send<W>(&self, stream: &mut W) -> io::Result<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        match self {
            ClientAuthStrategy::NoAuth => {
                // noop
            }
            ClientAuthStrategy::Token(token) => {
                // Unlike the standard network protocol, we write auth tokens as a fixed-size
                // header.
                stream.write_all(token.raw_bytes()).await?;
            }
        }
        Ok(())
    }
}

impl From<Option<ClientToken>> for ClientAuthStrategy {
    fn from(opt_token: Option<ClientToken>) -> Self {
        match opt_token {
            Some(token) => ClientAuthStrategy::Token(token),
            None => ClientAuthStrategy::NoAuth,
        }
    }
}

#[derive(Clone, Copy)]
pub enum ServerAuthStrategy {
    /// Expects no authentication header.
    NoAuth,
    /// The server expects a constant [AuthToken].
    Token(ClientToken),
}

impl ServerAuthStrategy {
    /// Verifies that a readable stream, intended to be a connection to a client, has written a
    /// suitable auth credential in its header. If authentication or authorization fails, an error
    /// is returned.
    /// This should be the first read done when accepting a client connection.
    pub fn verify_header(&self, stream: &mut impl Read) -> io::Result<()> {
        match self {
            ServerAuthStrategy::NoAuth => {
                // noop
            }
            ServerAuthStrategy::Token(token) => {
                // Unlike the standard network protocol, we read auth tokens as a fixed-size
                // header.
                let mut buf: [u8; TOKEN_LEN] = [0; TOKEN_LEN];
                stream.read_exact(&mut buf)?;
                Self::verify_header_help(buf, token)?;
            }
        }
        Ok(())
    }

    /// Like [Self::verify_header], but async.
    pub async fn async_verify_header<R>(&self, stream: &mut R) -> io::Result<()>
    where
        R: AsyncReadExt + Unpin,
    {
        match self {
            ServerAuthStrategy::NoAuth => {
                // noop
            }
            ServerAuthStrategy::Token(token) => {
                // Unlike the standard network protocol, we read auth tokens as a fixed-size
                // header.
                let mut buf: [u8; TOKEN_LEN] = [0; TOKEN_LEN];
                stream.read_exact(&mut buf).await?;
                Self::verify_header_help(buf, token)?;
            }
        }
        Ok(())
    }

    fn verify_header_help(
        read_token_buf: [u8; TOKEN_LEN],
        expect_token: &ClientToken,
    ) -> io::Result<()> {
        let token = ClientToken(Token(read_token_buf));
        if &token != expect_token {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "invalid auth header",
            ));
        }
        Ok(())
    }
}

impl From<Option<ClientToken>> for ServerAuthStrategy {
    fn from(opt_token: Option<ClientToken>) -> Self {
        match opt_token {
            Some(token) => ServerAuthStrategy::Token(token),
            None => ServerAuthStrategy::NoAuth,
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        future::Future,
        io::{self, Write},
        net::{TcpListener, TcpStream},
    };

    use tokio::io::AsyncWriteExt;

    use crate::auth::token::TOKEN_LEN;

    use super::{ClientAuthStrategy, ServerAuthStrategy};

    fn with_server_client(f: impl FnOnce(TcpStream, TcpStream)) {
        let server = TcpListener::bind("0.0.0.0:0").unwrap();

        let client_conn = TcpStream::connect(server.local_addr().unwrap()).unwrap();
        let (server_conn, _) = server.accept().unwrap();

        f(server_conn, client_conn);
    }

    async fn with_server_client_async<Fut, F>(f: F)
    where
        Fut: Future<Output = ()>,
        F: FnOnce(tokio::net::TcpStream, tokio::net::TcpStream) -> Fut,
    {
        use tokio::net::{TcpListener, TcpStream};
        let server = TcpListener::bind("0.0.0.0:0").await.unwrap();

        let client_conn = TcpStream::connect(server.local_addr().unwrap())
            .await
            .unwrap();
        let (server_conn, _) = server.accept().await.unwrap();

        f(server_conn, client_conn).await;
    }

    #[test]
    fn server_accept_client_auth_token() {
        with_server_client(|mut server, mut client| {
            let token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::Token(token);
            let client_auth = ClientAuthStrategy::Token(token);

            client_auth.send(&mut client).unwrap();
            server_auth.verify_header(&mut server).unwrap();
        });
    }

    #[tokio::test]
    async fn async_server_accept_client_auth_token() {
        with_server_client_async(|mut server, mut client| async move {
            let token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::Token(token);
            let client_auth = ClientAuthStrategy::Token(token);

            client_auth.async_send(&mut client).await.unwrap();
            server_auth.async_verify_header(&mut server).await.unwrap();
        })
        .await
    }

    #[test]
    fn server_reject_unsent_client_auth_token() {
        with_server_client(|mut server, client| {
            let token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::Token(token);

            drop(client);

            assert!(server_auth.verify_header(&mut server).is_err());
        });
    }

    #[tokio::test]
    async fn async_server_reject_unsent_client_auth_token() {
        with_server_client_async(|mut server, client| async move {
            let token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::Token(token);

            drop(client);

            assert!(server_auth.async_verify_header(&mut server).await.is_err());
        })
        .await
    }

    #[test]
    fn server_reject_incorrect_client_auth_token() {
        with_server_client(|mut server, mut client| {
            let server_auth =
                ServerAuthStrategy::Token("abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap());
            let client_auth =
                ClientAuthStrategy::Token("abqs_C4vQCjejjjdzwEh5tP7kag8C16iQh3".parse().unwrap());

            client_auth.send(&mut client).unwrap();
            let result = server_auth.verify_header(&mut server);
            let err = result.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        });
    }

    #[tokio::test]
    async fn async_server_reject_incorrect_client_auth_token() {
        with_server_client_async(|mut server, mut client| async move {
            let server_auth =
                ServerAuthStrategy::Token("abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap());
            let client_auth =
                ClientAuthStrategy::Token("abqs_C4vQCjejjjdzwEh5tP7kag8C16iQh3".parse().unwrap());

            client_auth.async_send(&mut client).await.unwrap();
            let result = server_auth.async_verify_header(&mut server).await;
            let err = result.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        })
        .await
    }

    #[test]
    fn server_reject_incorrect_client_auth_token_sent_too_short() {
        with_server_client(|mut server, mut client| {
            let token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::Token(token);

            client
                .write_all(&token.raw_bytes()[0..TOKEN_LEN - 1])
                .unwrap();
            drop(client);

            let result = server_auth.verify_header(&mut server);
            assert!(result.is_err());
        });
    }

    #[tokio::test]
    async fn async_server_reject_incorrect_client_auth_token_sent_too_short() {
        with_server_client_async(|mut server, mut client| async move {
            let token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::Token(token);

            client
                .write_all(&token.raw_bytes()[0..TOKEN_LEN - 1])
                .await
                .unwrap();
            drop(client);

            let result = server_auth.async_verify_header(&mut server).await;
            assert!(result.is_err());
        })
        .await
    }

    #[test]
    fn server_reject_incorrect_client_auth_token_sent_with_protocol_header() {
        with_server_client(|mut server, mut client| {
            let token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::Token(token);

            crate::net_protocol::write(&mut client, token.raw_bytes()).unwrap();
            drop(client);

            let result = server_auth.verify_header(&mut server);
            assert!(result.is_err());
        });
    }

    #[tokio::test]
    async fn async_server_reject_incorrect_client_auth_token_sent_with_protocol_header() {
        with_server_client_async(|mut server, mut client| async move {
            let token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::Token(token);

            crate::net_protocol::async_write(&mut client, &token.raw_bytes())
                .await
                .unwrap();
            drop(client);

            let result = server_auth.async_verify_header(&mut server).await;
            assert!(result.is_err());
        })
        .await
    }
}
