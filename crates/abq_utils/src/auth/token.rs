//! Generic implementations of authentication tokens.

use std::fmt;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use thiserror::Error;

pub(crate) const TOKEN_LEN: usize = 30;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct RawToken(pub(crate) [u8; TOKEN_LEN]);

#[derive(Debug, Error)]
pub(crate) enum TokenError {
    #[error("invalid auth token")]
    InvalidToken,
}

impl RawToken {
    /// Creates a new, randomly generated token.
    pub(crate) fn new_random() -> Self {
        // Use an RNG backed by 20-round ChaCha cipher. That should be more than enough security
        // for our needs; see also https://www.rfc-editor.org/rfc/rfc7539#section-1.
        // We seed from `getrandom` (which in turn draws from urandom, at least on Unix), which
        // again should be more than enough for our need.
        let mut rng: ChaCha20Rng = SeedableRng::from_entropy();
        let mut buf: [u8; 30] = [0; TOKEN_LEN];
        #[allow(clippy::needless_range_loop)] // IMO it's clearer to have a for-loop here
        for i in 0..TOKEN_LEN {
            buf[i] = rng.sample(rand::distributions::Alphanumeric);
        }

        Self(buf)
    }

    pub(crate) fn from_buf(buf: [u8; TOKEN_LEN]) -> Result<Self, TokenError> {
        for c in buf {
            if !(c as char).is_ascii_alphanumeric() {
                return Err(TokenError::InvalidToken);
            }
        }

        Ok(Self(buf))
    }

    pub(crate) fn parse_human_readable(s: &str, prefix: &str) -> Result<Self, TokenError> {
        use TokenError::*;

        let mut parts = s.split(prefix);
        let before = parts.next().ok_or(InvalidToken)?;
        if !before.is_empty() {
            return Err(InvalidToken);
        }

        let after = parts.next().ok_or(InvalidToken)?;
        let bytes = after.as_bytes();
        if bytes.len() != TOKEN_LEN {
            return Err(InvalidToken);
        }

        let mut owned_bytes = [0; TOKEN_LEN];
        owned_bytes[..TOKEN_LEN].copy_from_slice(&bytes[..TOKEN_LEN]);

        Self::from_buf(owned_bytes)
    }

    pub(crate) fn display_human_readable(
        &self,
        f: &mut fmt::Formatter<'_>,
        prefix: &str,
    ) -> fmt::Result {
        write!(f, "{}", prefix)?;
        for c in self.0 {
            fmt::Write::write_char(f, c as char)?
        }
        Ok(())
    }
}
