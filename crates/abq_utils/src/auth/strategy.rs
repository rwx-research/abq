use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::auth::{token::TOKEN_LEN, UserToken};

use std::{
    io::{self, Read, Write},
    marker::PhantomData,
};

use super::{token::RawToken, AdminToken};

#[derive(Clone, Copy)]
pub struct User;

#[derive(Clone, Copy)]
pub struct Admin;

pub fn build_strategies(
    user_token: UserToken,
    admin_token: AdminToken,
) -> (
    ServerAuthStrategy,
    ClientAuthStrategy<User>,
    ClientAuthStrategy<Admin>,
) {
    (
        ServerAuthStrategy::from_set(user_token, admin_token),
        ClientAuthStrategy::for_user(user_token),
        ClientAuthStrategy::for_admin(admin_token),
    )
}

#[derive(Clone, Copy)]
enum ClientAuthStrategyInner {
    /// The client sends no auth, and expects the server not to check for any.
    NoAuth,
    /// The client sends a constant [RawToken] to the server.
    /// This may only be constructed from a [UserToken] or [AdminToken].
    Token(RawToken),
}

/// Strategy for a request-sending client.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct ClientAuthStrategy<Role>(ClientAuthStrategyInner, PhantomData<Role>);

impl ClientAuthStrategy<User> {
    pub fn for_user(user_token: UserToken) -> Self {
        Self(
            ClientAuthStrategyInner::Token(user_token.0),
            Default::default(),
        )
    }

    pub fn no_auth() -> Self {
        Self(ClientAuthStrategyInner::NoAuth, Default::default())
    }
}

impl ClientAuthStrategy<Admin> {
    pub fn for_admin(admin_token: AdminToken) -> Self {
        Self(
            ClientAuthStrategyInner::Token(admin_token.0),
            Default::default(),
        )
    }
}

impl<T> ClientAuthStrategy<T> {
    /// Attaches auth as a constant header to a writable stream, intended to be a connection to a
    /// server.
    /// This should be the first write done in a connection to a server.
    pub fn send(&self, stream: &mut impl Write) -> io::Result<()> {
        match self.0 {
            ClientAuthStrategyInner::NoAuth => {
                // noop
            }
            ClientAuthStrategyInner::Token(token) => {
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
        match self.0 {
            ClientAuthStrategyInner::NoAuth => {
                // noop
            }
            ClientAuthStrategyInner::Token(token) => {
                // Unlike the standard network protocol, we write auth tokens as a fixed-size
                // header.
                stream.write_all(&token.0).await?;
            }
        }
        Ok(())
    }
}

impl From<Option<UserToken>> for ClientAuthStrategy<User> {
    fn from(opt_token: Option<UserToken>) -> Self {
        let strategy = match opt_token {
            Some(token) => ClientAuthStrategyInner::Token(token.0),
            None => ClientAuthStrategyInner::NoAuth,
        };
        Self(strategy, Default::default())
    }
}

// Enum to avoid direct construction of a role outside of this module!
#[derive(Debug, Clone, Copy)]
enum RoleInner {
    /// Client role.
    Client,
    /// Admin role, which includes everything in the client role.
    Admin,
}

/// The permissible role of a entity accepted by a [ServerAuthStrategy].
#[must_use]
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct Role(RoleInner);

impl Role {
    pub fn is_client(&self) -> bool {
        match self.0 {
            RoleInner::Client => true,
            RoleInner::Admin => true,
        }
    }

    pub fn is_admin(&self) -> bool {
        match self.0 {
            RoleInner::Admin => true,
            RoleInner::Client => false,
        }
    }
}

/// Set of tokens for different roles expected by a server.
#[derive(Clone, Copy)]
struct TokenSet {
    /// Token for a client.
    user_token: UserToken,

    /// Token for an administrator.
    admin_token: AdminToken,
}

#[derive(Clone, Copy)]
enum ServerAuthStrategyInner {
    /// Expects no authentication headers.
    NoAuth,
    /// The server expects tokens for clients and administrators.
    Token(TokenSet),
}

impl From<ServerAuthStrategyInner> for ServerAuthStrategy {
    fn from(strategy: ServerAuthStrategyInner) -> Self {
        Self(strategy)
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct ServerAuthStrategy(ServerAuthStrategyInner);

impl ServerAuthStrategy {
    pub fn no_auth() -> Self {
        ServerAuthStrategyInner::NoAuth.into()
    }

    pub fn from_set(user_token: UserToken, admin_token: AdminToken) -> Self {
        ServerAuthStrategyInner::Token(TokenSet {
            user_token,
            admin_token,
        })
        .into()
    }

    /// Verifies that a readable stream, intended to be a connection to a client or admin, has
    /// written a suitable auth credential in its header.
    ///
    /// Returns the [Role] the connection is determined to be.
    ///
    /// If authentication or authorization fails, an error is returned.
    /// This should be the first read done when accepting a client connection.
    pub fn verify_header(&self, stream: &mut impl Read) -> io::Result<Role> {
        use ServerAuthStrategyInner::*;

        match &self.0 {
            NoAuth => {
                // noop, everyone is an admin in this world
                Ok(Role(RoleInner::Admin))
            }
            Token(token_set) => {
                // Unlike the standard network protocol, we read auth tokens as a fixed-size
                // header.
                let mut buf: [u8; TOKEN_LEN] = [0; TOKEN_LEN];
                stream.read_exact(&mut buf)?;
                Self::verify_header_help(buf, token_set)
            }
        }
    }

    /// Like [Self::verify_header], but async.
    pub async fn async_verify_header<R>(&self, stream: &mut R) -> io::Result<Role>
    where
        R: AsyncReadExt + Unpin,
    {
        use ServerAuthStrategyInner::*;

        match &self.0 {
            NoAuth => {
                // noop, everyone is an admin in this world
                Ok(Role(RoleInner::Admin))
            }
            Token(token_set) => {
                // Unlike the standard network protocol, we read auth tokens as a fixed-size
                // header.
                let mut buf: [u8; TOKEN_LEN] = [0; TOKEN_LEN];
                stream.read_exact(&mut buf).await?;
                Self::verify_header_help(buf, token_set)
            }
        }
    }

    fn verify_header_help(
        read_token_buf: [u8; TOKEN_LEN],
        token_set: &TokenSet,
    ) -> io::Result<Role> {
        // We expect to get many more client than admin connections, so always check client first.
        {
            let client_token = UserToken(RawToken(read_token_buf));
            if client_token == token_set.user_token {
                return Ok(Role(RoleInner::Client));
            }
        }

        {
            let admin_token = AdminToken(RawToken(read_token_buf));
            if admin_token == token_set.admin_token {
                return Ok(Role(RoleInner::Admin));
            }
        }

        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "invalid auth header",
        ))
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

    use crate::auth::{token::TOKEN_LEN, AdminToken, UserToken};

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
    fn server_accept_user_auth_token() {
        with_server_client(|mut server, mut client| {
            let user_token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::from_set(user_token, AdminToken::new_random());
            let client_auth = ClientAuthStrategy::for_user(user_token);

            client_auth.send(&mut client).unwrap();
            let role = server_auth.verify_header(&mut server).unwrap();
            assert!(role.is_client() && !role.is_admin());
        });
    }

    #[tokio::test]
    async fn async_server_accept_user_auth_token() {
        with_server_client_async(|mut server, mut client| async move {
            let user_token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::from_set(user_token, AdminToken::new_random());
            let client_auth = ClientAuthStrategy::for_user(user_token);

            client_auth.async_send(&mut client).await.unwrap();
            let role = server_auth.async_verify_header(&mut server).await.unwrap();
            assert!(role.is_client() && !role.is_admin());
        })
        .await
    }

    #[test]
    fn server_reject_unsent_user_auth_token() {
        with_server_client(|mut server, client| {
            let user_token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::from_set(user_token, AdminToken::new_random());

            drop(client);

            assert!(server_auth.verify_header(&mut server).is_err());
        });
    }

    #[tokio::test]
    async fn async_server_reject_unsent_user_auth_token() {
        with_server_client_async(|mut server, client| async move {
            let user_token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::from_set(user_token, AdminToken::new_random());

            drop(client);

            assert!(server_auth.async_verify_header(&mut server).await.is_err());
        })
        .await
    }

    #[test]
    fn server_reject_incorrect_user_auth_token() {
        with_server_client(|mut server, mut client| {
            let server_auth = ServerAuthStrategy::from_set(
                "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap(),
                AdminToken::new_random(),
            );
            let client_auth = ClientAuthStrategy::for_user(
                "abqs_C4vQCjejjjdzwEh5tP7kag8C16iQh3".parse().unwrap(),
            );

            client_auth.send(&mut client).unwrap();
            let result = server_auth.verify_header(&mut server);
            let err = result.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        });
    }

    #[tokio::test]
    async fn async_server_reject_incorrect_user_auth_token() {
        with_server_client_async(|mut server, mut client| async move {
            let server_auth = ServerAuthStrategy::from_set(
                "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap(),
                AdminToken::new_random(),
            );
            let client_auth = ClientAuthStrategy::for_user(
                "abqs_C4vQCjejjjdzwEh5tP7kag8C16iQh3".parse().unwrap(),
            );

            client_auth.async_send(&mut client).await.unwrap();
            let result = server_auth.async_verify_header(&mut server).await;
            let err = result.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        })
        .await
    }

    #[test]
    fn server_reject_incorrect_user_auth_token_sent_too_short() {
        with_server_client(|mut server, mut client| {
            let user_token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::from_set(user_token, AdminToken::new_random());

            client
                .write_all(&user_token.raw_bytes()[0..TOKEN_LEN - 1])
                .unwrap();
            drop(client);

            let result = server_auth.verify_header(&mut server);
            assert!(result.is_err());
        });
    }

    #[tokio::test]
    async fn async_server_reject_incorrect_user_auth_token_sent_too_short() {
        with_server_client_async(|mut server, mut client| async move {
            let user_token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::from_set(user_token, AdminToken::new_random());

            client
                .write_all(&user_token.raw_bytes()[0..TOKEN_LEN - 1])
                .await
                .unwrap();
            drop(client);

            let result = server_auth.async_verify_header(&mut server).await;
            assert!(result.is_err());
        })
        .await
    }

    #[test]
    fn server_reject_incorrect_user_auth_token_sent_with_protocol_header() {
        with_server_client(|mut server, mut client| {
            let user_token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::from_set(user_token, AdminToken::new_random());

            crate::net_protocol::write(&mut client, user_token.raw_bytes()).unwrap();
            drop(client);

            let result = server_auth.verify_header(&mut server);
            assert!(result.is_err());
        });
    }

    #[tokio::test]
    async fn async_server_reject_incorrect_user_auth_token_sent_with_protocol_header() {
        with_server_client_async(|mut server, mut client| async move {
            let user_token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::from_set(user_token, AdminToken::new_random());

            crate::net_protocol::async_write(&mut client, &user_token.raw_bytes())
                .await
                .unwrap();
            drop(client);

            let result = server_auth.async_verify_header(&mut server).await;
            assert!(result.is_err());
        })
        .await
    }

    #[test]
    fn server_accept_admin_auth_token() {
        with_server_client(|mut server, mut client| {
            let admin_token = "abqadmin_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::from_set(UserToken::new_random(), admin_token);
            let client_auth = ClientAuthStrategy::for_admin(admin_token);

            client_auth.send(&mut client).unwrap();
            let role = server_auth.verify_header(&mut server).unwrap();
            assert!(role.is_admin() && role.is_client());
        });
    }

    #[tokio::test]
    async fn async_server_accept_admin_auth_token() {
        with_server_client_async(|mut server, mut client| async move {
            let admin_token = "abqadmin_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::from_set(UserToken::new_random(), admin_token);
            let client_auth = ClientAuthStrategy::for_admin(admin_token);

            client_auth.async_send(&mut client).await.unwrap();
            let role = server_auth.async_verify_header(&mut server).await.unwrap();
            assert!(role.is_admin() && role.is_client());
        })
        .await
    }

    #[test]
    fn server_reject_unsent_admin_auth_token() {
        with_server_client(|mut server, client| {
            let admin_token = "abqadmin_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::from_set(UserToken::new_random(), admin_token);

            drop(client);

            assert!(server_auth.verify_header(&mut server).is_err());
        });
    }

    #[tokio::test]
    async fn async_server_reject_unsent_admin_auth_token() {
        with_server_client_async(|mut server, client| async move {
            let user_token = "abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::from_set(user_token, AdminToken::new_random());

            drop(client);

            assert!(server_auth.async_verify_header(&mut server).await.is_err());
        })
        .await
    }

    #[test]
    fn server_reject_incorrect_admin_auth_token() {
        with_server_client(|mut server, mut client| {
            let server_auth = ServerAuthStrategy::from_set(
                UserToken::new_random(),
                "abqadmin_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap(),
            );
            let client_auth = ClientAuthStrategy::for_admin(
                "abqadmin_C4vQCjejjjdzwEh5tP7kag8C16iQh3".parse().unwrap(),
            );

            client_auth.send(&mut client).unwrap();
            let result = server_auth.verify_header(&mut server);
            let err = result.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        });
    }

    #[tokio::test]
    async fn async_server_reject_incorrect_admin_auth_token() {
        with_server_client_async(|mut server, mut client| async move {
            let server_auth = ServerAuthStrategy::from_set(
                UserToken::new_random(),
                "abqadmin_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap(),
            );
            let client_auth = ClientAuthStrategy::for_admin(
                "abqadmin_C4vQCjejjjdzwEh5tP7kag8C16iQh3".parse().unwrap(),
            );

            client_auth.async_send(&mut client).await.unwrap();
            let result = server_auth.async_verify_header(&mut server).await;
            let err = result.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        })
        .await
    }

    #[test]
    fn server_reject_incorrect_admin_auth_token_sent_too_short() {
        with_server_client(|mut server, mut client| {
            let admin_token = "abqadmin_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::from_set(UserToken::new_random(), admin_token);

            client
                .write_all(&admin_token.raw_bytes()[0..TOKEN_LEN - 1])
                .unwrap();
            drop(client);

            let result = server_auth.verify_header(&mut server);
            assert!(result.is_err());
        });
    }

    #[tokio::test]
    async fn async_server_reject_incorrect_admin_auth_token_sent_too_short() {
        with_server_client_async(|mut server, mut client| async move {
            let admin_token = "abqadmin_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::from_set(UserToken::new_random(), admin_token);

            client
                .write_all(&admin_token.raw_bytes()[0..TOKEN_LEN - 1])
                .await
                .unwrap();
            drop(client);

            let result = server_auth.async_verify_header(&mut server).await;
            assert!(result.is_err());
        })
        .await
    }

    #[test]
    fn server_reject_incorrect_admin_auth_token_sent_with_protocol_header() {
        with_server_client(|mut server, mut client| {
            let admin_token = "abqadmin_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::from_set(UserToken::new_random(), admin_token);

            crate::net_protocol::write(&mut client, admin_token.raw_bytes()).unwrap();
            drop(client);

            let result = server_auth.verify_header(&mut server);
            assert!(result.is_err());
        });
    }

    #[tokio::test]
    async fn async_server_reject_incorrect_admin_auth_token_sent_with_protocol_header() {
        with_server_client_async(|mut server, mut client| async move {
            let admin_token = "abqadmin_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF".parse().unwrap();
            let server_auth = ServerAuthStrategy::from_set(UserToken::new_random(), admin_token);

            crate::net_protocol::async_write(&mut client, &admin_token.raw_bytes())
                .await
                .unwrap();
            drop(client);

            let result = server_auth.async_verify_header(&mut server).await;
            assert!(result.is_err());
        })
        .await
    }
}
