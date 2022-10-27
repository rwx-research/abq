//! Utilities for getting and validating worker/supervisor authz credentials for
//! requests to, and accepted by, abq servers.

use core::fmt;
use std::str::FromStr;

use thiserror::Error;

use super::token::{Token, TokenError};

const CLIENT_DISPLAY_PREFIX: &str = "abqs_";

/// A token representing a "client" role for authz against a server.
/// A client is an ABQ worker or supervisor, which can communicate freely with an ABQ queue server
/// in all regards except admin requests.
///
/// The token is GitHub's model - we take 30 bits from the ASCII alphanumeric alphabet [a, z] U [A, Z] U [0, 9].
/// That gives us log_2(62^30) ~= 178 bits of entropy, more than enough for our needs right now.
///
/// Tokens are exposed to users, and parsed from user input, with an `abqs_` prefix.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ClientToken(pub(crate) Token);

#[derive(Debug, Error)]
pub enum ClientTokenError {
    #[error("invalid auth token")]
    InvalidToken,
}

impl From<TokenError> for ClientTokenError {
    fn from(te: TokenError) -> Self {
        match te {
            TokenError::InvalidToken => Self::InvalidToken,
        }
    }
}

impl ClientToken {
    /// Creates a new, randomly generated token.
    pub fn new_random() -> Self {
        Self(Token::new_random())
    }

    pub(crate) fn raw_bytes(&self) -> &[u8] {
        &self.0 .0
    }
}

impl fmt::Display for ClientToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.display_human_readable(f, CLIENT_DISPLAY_PREFIX)
    }
}

impl FromStr for ClientToken {
    type Err = ClientTokenError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Token::parse_human_readable(s, CLIENT_DISPLAY_PREFIX)?))
    }
}

#[cfg(test)]
mod test {
    use super::ClientToken;

    use std::str::FromStr;

    #[test]
    fn generated_auth_token_observes_properties() {
        // Maybe we can bring in property testing later, but for now just generate 1000 tokens at a
        // time, and make sure they
        //   - are 30 bytes in length
        //   - are alphanumeric
        // For their string representation, check the above, and that
        //   - they start with `abqs_`
        for _ in 0..1_000 {
            let token = ClientToken::new_random();
            assert_eq!(token.raw_bytes().len(), 30);
            assert!(token.raw_bytes().iter().all(|c| c.is_ascii_alphanumeric()));

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
        let token = ClientToken::from_str("abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF").unwrap();
        assert_eq!(
            token.raw_bytes(),
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
        let result = ClientToken::from_str("abqs_MD2QPKH2VZU2krvOa2mN54Q4qwz");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_token_too_long() {
        let result = ClientToken::from_str("abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzzzzzz");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_token_not_alphanumeric() {
        let result = ClientToken::from_str("abqs_MD2QPKH2VZU2krvOa2mN54Q4qwz_xF");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_token_not_ascii_alphanumeric() {
        let result = ClientToken::from_str("abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNx京");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_token_no_abqs_prefix() {
        let result = ClientToken::from_str("MD2QPKH2VZU2krvOa2mN54Q4qwzNxF");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_token_malformed_abqs_prefix() {
        let result = ClientToken::from_str("abqsMD2QPKH2VZU2krvOa2mN54Q4qwzNxF");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_token_abqs_prefix_in_middle_of_token() {
        let result = ClientToken::from_str("MD2QPKH2V_abqs_ZU2krvOa2mN54Q4qwzNxF");
        assert!(result.is_err());
    }
}
