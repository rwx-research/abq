//! Utilities for interacting with the queue config API

use core::fmt;
use std::str::FromStr;
use thiserror::Error;

const API_KEY_LENGTH: usize = 30;

/// An API key a client can use to pull configuration information (like the queue address) from a hosted ABQ API.
/// Right now these keys are not used at all, since the configuration information is hard coded in the ABQ binary,
/// and the hosted ABQ API does not yet exist.
///
/// The key is GitHub's model - we take 30 bits from the ASCII alphanumeric alphabet [a, z] U [A, Z] U [0, 9].
/// That gives us log_2(62^30) ~= 178 bits of entropy, more than enough for our needs right now.
///
/// keys are exposed to users, and parsed from user input, with an `abqapi_` prefix.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ApiKey([u8; API_KEY_LENGTH]);

#[derive(Debug, Error)]
pub enum ApiKeyError {
    #[error("invalid api key")]
    InvalidApiKey,
}

impl ApiKey {
    fn from_buf(buf: [u8; API_KEY_LENGTH]) -> Result<Self, ApiKeyError> {
        for c in buf {
            if !(c as char).is_ascii_alphanumeric() {
                return Err(ApiKeyError::InvalidApiKey);
            }
        }

        Ok(Self(buf))
    }
}

impl fmt::Display for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "abqapi_")?;
        for c in self.0 {
            fmt::Write::write_char(f, c as char)?
        }
        Ok(())
    }
}

impl FromStr for ApiKey {
    type Err = ApiKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use ApiKeyError::*;

        let mut parts = s.split("abqapi_");
        let before = parts.next().ok_or(InvalidApiKey)?;
        if !before.is_empty() {
            return Err(InvalidApiKey);
        }

        let after = parts.next().ok_or(InvalidApiKey)?;
        let bytes = after.as_bytes();
        if bytes.len() != API_KEY_LENGTH {
            return Err(InvalidApiKey);
        }

        let mut owned_bytes = [0; API_KEY_LENGTH];
        owned_bytes[..API_KEY_LENGTH].copy_from_slice(&bytes[..API_KEY_LENGTH]);

        Self::from_buf(owned_bytes)
    }
}

#[cfg(test)]
mod test {
    use super::ApiKey;

    use std::str::FromStr;

    #[test]
    fn parse_valid_key() {
        let key = ApiKey::from_str("abqapi_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF").unwrap();
        assert_eq!(
            key.0,
            [
                b'M', b'D', b'2', b'Q', b'P', b'K', b'H', b'2', b'V', b'Z', b'U', b'2', b'k', b'r',
                b'v', b'O', b'a', b'2', b'm', b'N', b'5', b'4', b'Q', b'4', b'q', b'w', b'z', b'N',
                b'x', b'F'
            ]
        );
        assert_eq!(key.to_string(), "abqapi_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF");
    }

    #[test]
    fn reject_string_key_too_short() {
        let result = ApiKey::from_str("abqapi_MD2QPKH2VZU2krvOa2mN54Q4qwz");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_key_too_long() {
        let result = ApiKey::from_str("abqapi_MD2QPKH2VZU2krvOa2mN54Q4qwzzzzzz");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_key_not_alphanumeric() {
        let result = ApiKey::from_str("abqapi_MD2QPKH2VZU2krvOa2mN54Q4qwz_xF");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_key_not_ascii_alphanumeric() {
        let result = ApiKey::from_str("abqapi_MD2QPKH2VZU2krvOa2mN54Q4qwzNx京");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_key_no_abqapi_prefix() {
        let result = ApiKey::from_str("MD2QPKH2VZU2krvOa2mN54Q4qwzNxF");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_key_malformed_abqapi_prefix() {
        let result = ApiKey::from_str("abqsMD2QPKH2VZU2krvOa2mN54Q4qwzNxF");
        assert!(result.is_err());
    }

    #[test]
    fn reject_string_key_abqapi_prefix_in_middle_of_key() {
        let result = ApiKey::from_str("MD2QPKH2V_abqapi_ZU2krvOa2mN54Q4qwzNxF");
        assert!(result.is_err());
    }
}
