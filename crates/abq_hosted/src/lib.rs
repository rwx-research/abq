use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;

use abq_utils::auth::AuthToken;
use abq_utils::net_protocol::workers::RunId;
use abq_utils::{api::ApiKey, net_opt::Tls};
use reqwest::StatusCode;
use serde::Deserialize;
use thiserror::Error;
use url::Url;

pub const DEFAULT_RWX_ABQ_API_URL: &str = "https://captain.build/abq/api/";

#[derive(Debug)]
pub struct HostedQueueConfig {
    pub addr: SocketAddr,
    pub run_id: RunId,
    pub auth_token: AuthToken,
    pub tls: Tls,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid url {0}")]
    InvalidUrl(String),
    #[error("could not authenticate against the hosted API")]
    Unauthenticated,
    #[error("could not authorize against the hosted API")]
    Unauthorized,
    #[error("hosted API returned an unexpected schema: {0}")]
    SchemaError(String),
    #[error("hosted API returned an unexpected error: {0}")]
    Other(String),
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        match e.status() {
            Some(code) => {
                if code == StatusCode::UNAUTHORIZED {
                    Self::Unauthenticated
                } else if code == StatusCode::FORBIDDEN {
                    Self::Unauthorized
                } else {
                    Self::Other(e.to_string())
                }
            }
            None => Self::Other(e.to_string()),
        }
    }
}

#[derive(Deserialize, Debug)]
struct HostedQueueResponse {
    queue_url: Url,
}

impl HostedQueueConfig {
    pub fn from_api<U>(api_url: U, api_key: ApiKey, run_id: &RunId) -> Result<Self, Error>
    where
        U: AsRef<str>,
    {
        let queue_api = Url::try_from(api_url.as_ref())
            .map_err(|e| Error::InvalidUrl(e.to_string()))
            .and_then(|mut api| {
                // Really unfortunate API here. See https://github.com/servo/rust-url/issues/333;
                // perhaps it will improve in the future.
                api.path_segments_mut()
                    .map_err(|()| Error::InvalidUrl(api_url.as_ref().to_string()))?
                    .push("queue");
                Ok(api)
            })?;

        let client = reqwest::blocking::Client::new();
        let resp: HostedQueueResponse = client
            .get(queue_api)
            .bearer_auth(api_key)
            .query(&[("run_id", run_id.to_string())])
            .send()?
            .error_for_status()?
            .json()?;

        let HostedQueueResponse { queue_url } = resp;

        let tls = match queue_url.scheme() {
            "abq" => Tls::NO,
            "abqs" => Tls::YES,
            s => return Err(Error::SchemaError(format!("invalid URL schema {s}"))),
        };

        let addr = match queue_url.socket_addrs(|| None).as_deref() {
            Ok([one]) => *one,
            Ok(_) => {
                return Err(Error::SchemaError(
                    "expected exactly one resolved queue address".to_string(),
                ))
            }
            Err(e) => return Err(Error::SchemaError(e.to_string())),
        };

        let queries: HashMap<_, _> = queue_url.query_pairs().collect();

        let auth_token = queries
            .get("token")
            .ok_or_else(|| Error::SchemaError("missing token".to_owned()))
            .and_then(|token_str| {
                AuthToken::from_str(token_str)
                    .map_err(|e| Error::SchemaError(format!("invalid token: {e}")))
            })?;

        let resolved_run_id = queries
            .get("run_id")
            .map(|id| RunId(id.to_string()))
            .ok_or_else(|| Error::SchemaError("missing run id".to_string()))?;

        if run_id != &resolved_run_id {
            return Err(Error::Other(
                "host-provided run ID differs from requested run ID".to_string(),
            ));
        }

        Ok(HostedQueueConfig {
            addr,
            run_id: resolved_run_id,
            auth_token,
            tls,
        })
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use abq_utils::{api::ApiKey, auth::AuthToken, net_opt::Tls, net_protocol::workers::RunId};

    use super::{Error, HostedQueueConfig};

    use mockito::{mock, Matcher};

    fn test_api_key() -> ApiKey {
        ApiKey::from_str("abqapi_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF").unwrap()
    }

    fn test_auth_token() -> AuthToken {
        AuthToken::from_str("abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF").unwrap()
    }

    #[test]
    fn get_hosted_queue_config_with_tls() {
        let in_run_id = RunId("1234".to_string());

        let _m = mock("GET", "/queue")
            .match_header(
                "Authorization",
                format!("Bearer {}", test_api_key()).as_str(),
            )
            .match_query(Matcher::AnyOf(vec![Matcher::UrlEncoded(
                "run_id".to_string(),
                in_run_id.to_string(),
            )]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"queue_url":"abqs://168.220.85.45:8080?run_id=1234\u0026token={}"}}"#,
                test_auth_token()
            ))
            .create();

        let HostedQueueConfig {
            addr,
            run_id,
            auth_token,
            tls,
        } = HostedQueueConfig::from_api(mockito::server_url(), test_api_key(), &in_run_id).unwrap();

        assert_eq!(addr, "168.220.85.45:8080".parse().unwrap());
        assert_eq!(run_id, in_run_id);
        assert_eq!(auth_token, test_auth_token());
        assert_eq!(tls, Tls::YES);
    }

    #[test]
    fn get_hosted_queue_config_without_tls() {
        let in_run_id = RunId("1234".to_string());

        let _m = mock("GET", "/queue")
            .match_header(
                "Authorization",
                format!("Bearer {}", test_api_key()).as_str(),
            )
            .match_query(Matcher::AnyOf(vec![Matcher::UrlEncoded(
                "run_id".to_string(),
                in_run_id.to_string(),
            )]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"queue_url":"abq://168.220.85.45:8080?run_id=1234\u0026token={}"}}"#,
                test_auth_token()
            ))
            .create();

        let HostedQueueConfig {
            addr,
            run_id,
            auth_token,
            tls,
        } = HostedQueueConfig::from_api(mockito::server_url(), test_api_key(), &in_run_id).unwrap();

        assert_eq!(addr, "168.220.85.45:8080".parse().unwrap());
        assert_eq!(run_id, in_run_id);
        assert_eq!(auth_token, test_auth_token());
        assert_eq!(tls, Tls::NO);
    }

    #[test]
    fn get_hosted_queue_config_wrong_authn() {
        let _m = mock("GET", "/queue")
            .match_query(Matcher::Any)
            .with_status(401)
            .create();

        let err = HostedQueueConfig::from_api(
            mockito::server_url(),
            test_api_key(),
            &RunId("".to_owned()),
        )
        .unwrap_err();

        assert!(matches!(err, Error::Unauthenticated));
    }

    #[test]
    fn get_hosted_queue_config_wrong_authz() {
        let _m = mock("GET", "/queue")
            .match_query(Matcher::Any)
            .with_status(403)
            .create();

        let err = HostedQueueConfig::from_api(
            mockito::server_url(),
            test_api_key(),
            &RunId("".to_owned()),
        )
        .unwrap_err();

        assert!(matches!(err, Error::Unauthorized));
    }

    #[test]
    fn get_hosted_queue_config_unexpected_response() {
        let in_run_id = RunId("1234".to_string());

        let _m = mock("GET", "/queue")
            .match_header(
                "Authorization",
                format!("Bearer {}", test_api_key()).as_str(),
            )
            .match_query(Matcher::AnyOf(vec![Matcher::UrlEncoded(
                "run_id".to_string(),
                in_run_id.to_string(),
            )]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"queue_url":"tcp://168.220.85.45:8080"}"#)
            .create();

        let err = HostedQueueConfig::from_api(mockito::server_url(), test_api_key(), &in_run_id)
            .unwrap_err();

        assert!(matches!(err, Error::SchemaError(..)));
    }

    #[test]
    fn get_hosted_queue_config_bad_api_url() {
        let err = HostedQueueConfig::from_api(
            "definitely not a url",
            test_api_key(),
            &RunId("".to_owned()),
        )
        .unwrap_err();

        assert!(matches!(err, Error::InvalidUrl(..)));
    }
}
