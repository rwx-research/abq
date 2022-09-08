//! Utilities for getting and validating authz credentials for requests to, and accepted by, abq
//! servers.

use std::io::{self, Write};

use tokio::io::AsyncWriteExt;

#[derive(Clone, Copy)]
pub enum ClientAuthStrategy {
    /// The client sends no auth, and expects the server not to check for any.
    NoAuth,
}

impl ClientAuthStrategy {
    /// Attaches auth as a constant header to a writable stream, intended to be a connection to a
    /// server.
    /// This should be the first write done in a connection to a server.
    pub fn send(&self, _stream: &mut impl Write) -> io::Result<()> {
        match self {
            ClientAuthStrategy::NoAuth => {
                // noop
            }
        }
        Ok(())
    }

    /// Like [Self::send], but async.
    pub async fn async_send(&self, _stream: &mut impl AsyncWriteExt) -> io::Result<()> {
        match self {
            ClientAuthStrategy::NoAuth => {
                // noop
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub enum ServerAuthStrategy {
    /// Expects no authentication header.
    NoAuth,
}

impl ServerAuthStrategy {
    /// Verifies that a readable stream, intended to be a connection to a client, has written a
    /// suitable auth credential in its header. If authentication or authorization fails, an error
    /// is returned.
    /// This should be the first read done when accepting a client connection.
    pub fn verify_header(&self, _stream: &mut impl Write) -> io::Result<()> {
        match self {
            ServerAuthStrategy::NoAuth => {
                // noop
            }
        }
        Ok(())
    }

    /// Like [Self::verify_header], but async.
    pub async fn async_verify_header(&self, _stream: &mut impl AsyncWriteExt) -> io::Result<()> {
        match self {
            ServerAuthStrategy::NoAuth => {
                // noop
            }
        }
        Ok(())
    }
}
