//! Utilities for getting and validating authz credentials for requests to, and accepted by, abq
//! servers.

use core::fmt;
use std::{
    io::{self, Read, Write},
    str::FromStr,
};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const AUTH_TOKEN_LEN: usize = 30;

/// A token a client can use to authenticate and authorize access to a server.
/// Right now this is nothing interesting; all abq servers use the same token per instance, and all
/// client authenticate with that same token. There are no roles distinguished by token.
///
/// The token is GitHub's model - we take 30 bits from the ASCII alphanumeric alphabet [a, z] U [A, Z] U [0, 9].
/// That gives us log_2(62^30) ~= 178 bits of entropy, more than enough for our needs right now.
///
/// Tokens are exposed to users, and parsed from user input, with an `abqs_` prefix.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct AuthToken([u8; AUTH_TOKEN_LEN]);

#[derive(Debug, Error)]
pub enum AuthTokenError {
    #[error("invalid auth token")]
    InvalidAuthToken,
}

impl AuthToken {
    /// Creates a new, randomly generated token.
    pub fn new_random() -> Self {
        // Use an RNG backed by 20-round ChaCha cipher. That should be more than enough security
        // for our needs; see also https://www.rfc-editor.org/rfc/rfc7539#section-1.
        // We seed from `getrandom` (which in turn draws from urandom, at least on Unix), which
        // again should be more than enough for our need.
        let mut rng: ChaCha20Rng = SeedableRng::from_entropy();
        let mut buf: [u8; 30] = [0; AUTH_TOKEN_LEN];
        #[allow(clippy::needless_range_loop)] // IMO it's clearer to have a for-loop here
        for i in 0..AUTH_TOKEN_LEN {
            buf[i] = rng.sample(rand::distributions::Alphanumeric);
        }

        Self(buf)
    }

    fn from_buf(buf: [u8; AUTH_TOKEN_LEN]) -> Result<Self, AuthTokenError> {
        for c in buf {
            if !(c as char).is_ascii_alphanumeric() {
                return Err(AuthTokenError::InvalidAuthToken);
            }
        }

        Ok(Self(buf))
    }
}

impl fmt::Display for AuthToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "abqs_")?;
        for c in self.0 {
            fmt::Write::write_char(f, c as char)?
        }
        Ok(())
    }
}

impl FromStr for AuthToken {
    type Err = AuthTokenError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use AuthTokenError::*;

        let mut parts = s.split("abqs_");
        let before = parts.next().ok_or(InvalidAuthToken)?;
        if !before.is_empty() {
            return Err(InvalidAuthToken);
        }

        let after = parts.next().ok_or(InvalidAuthToken)?;
        let bytes = after.as_bytes();
        if bytes.len() != AUTH_TOKEN_LEN {
            return Err(InvalidAuthToken);
        }

        let mut owned_bytes = [0; AUTH_TOKEN_LEN];
        owned_bytes[..AUTH_TOKEN_LEN].copy_from_slice(&bytes[..AUTH_TOKEN_LEN]);

        Self::from_buf(owned_bytes)
    }
}

#[derive(Clone, Copy)]
pub enum ClientAuthStrategy {
    /// The client sends no auth, and expects the server not to check for any.
    NoAuth,
    /// The client sends a constant [AuthToken] to the server.
    Token(AuthToken),
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
                stream.write_all(&token.0)?;
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
                stream.write_all(&token.0).await?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub enum ServerAuthStrategy {
    /// Expects no authentication header.
    NoAuth,
    /// The server expects a constant [AuthToken].
    Token(AuthToken),
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
                let mut buf: [u8; AUTH_TOKEN_LEN] = [0; AUTH_TOKEN_LEN];
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
                let mut buf: [u8; AUTH_TOKEN_LEN] = [0; AUTH_TOKEN_LEN];
                stream.read_exact(&mut buf).await?;
                Self::verify_header_help(buf, token)?;
            }
        }
        Ok(())
    }

    fn verify_header_help(
        read_token_buf: [u8; AUTH_TOKEN_LEN],
        expect_token: &AuthToken,
    ) -> io::Result<()> {
        let token = AuthToken(read_token_buf);
        if &token != expect_token {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "invalid auth header",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use tokio::io::AsyncWriteExt;

    use crate::auth::AUTH_TOKEN_LEN;

    use super::{AuthToken, ClientAuthStrategy, ServerAuthStrategy};

    use std::{
        future::Future,
        io::{self, Write},
        net::{TcpListener, TcpStream},
        str::FromStr,
    };

    #[test]
    fn generated_auth_token_observes_properties() {
        // Maybe we can bring in property testing later, but for now just generate 1000 tokens at a
        // time, and make sure they
        //   - are 30 bytes in length
        //   - are alphanumeric
        // For their string representation, check the above, and that
        //   - they start with `abqs_`
        for _ in 0..1_000 {
            let token = AuthToken::new_random();
            assert_eq!(token.0.len(), 30);
            assert!(token.0.iter().all(|c| c.is_ascii_alphanumeric()));

            let token_s = token.to_string();
            assert_eq!(token_s.len(), 30 + "abqs_".len());
            assert!(token_s.starts_with("abqs_"));
            assert!(token_s
                .chars()
                .skip("abqs_".len())
                .all(|c| c.is_ascii_alphanumeric()));

            let reparsed_token = token_s.parse().unwrap();
            assert!(token == reparsed_token);
        }
    }

    #[test]
    fn parse_valid_token() {
        let token = AuthToken::from_str("abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF").unwrap();
        assert_eq!(
            token.0,
            [
                b'M', b'D', b'2', b'Q', b'P', b'K', b'H', b'2', b'V', b'Z', b'U', b'2', b'k', b'r',
                b'v', b'O', b'a', b'2', b'm', b'N', b'5', b'4', b'Q', b'4', b'q', b'w', b'z', b'N',
                b'x', b'F'
            ]
        );
        assert_eq!(token.to_string(), "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF");
    }

    #[test]
    fn reject_string_token_too_short() {
        let result = AuthToken::from_str("abqs_MD2QPKH2VZU2krvOa2mN54Q4qwz");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_token_too_long() {
        let result = AuthToken::from_str("abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzzzzzz");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_token_not_alphanumeric() {
        let result = AuthToken::from_str("abqs_MD2QPKH2VZU2krvOa2mN54Q4qwz_xF");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_token_not_ascii_alphanumeric() {
        let result = AuthToken::from_str("abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNx京");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_token_no_abqs_prefix() {
        let result = AuthToken::from_str("MD2QPKH2VZU2krvOa2mN54Q4qwzNxF");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_token_malformed_abqs_prefix() {
        let result = AuthToken::from_str("abqsMD2QPKH2VZU2krvOa2mN54Q4qwzNxF");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_token_abqs_prefix_in_middle_of_token() {
        let result = AuthToken::from_str("MD2QPKH2V_abqs_ZU2krvOa2mN54Q4qwzNxF");
        assert!(result.is_err());
    }

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

            client.write_all(&token.0[0..AUTH_TOKEN_LEN - 1]).unwrap();
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
                .write_all(&token.0[0..AUTH_TOKEN_LEN - 1])
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

            crate::net_protocol::write(&mut client, token.0).unwrap();
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

            crate::net_protocol::async_write(&mut client, &token.0)
                .await
                .unwrap();
            drop(client);

            let result = server_auth.async_verify_header(&mut server).await;
            assert!(result.is_err());
        })
        .await
    }
}
