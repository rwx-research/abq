mod api_key;

pub use api_key::ApiKey;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::thread;
use std::time::Duration;

use abq_utils::auth::UserToken;
use abq_utils::net_opt::Tls;
use abq_utils::net_protocol::workers::RunId;
use reqwest::{blocking::RequestBuilder, StatusCode};
use serde::Deserialize;
use thiserror::Error;
use url::Url;

pub const DEFAULT_RWX_ABQ_API_URL: &str = "https://abq.build/api";

#[derive(Debug)]
pub struct HostedQueueConfig {
    pub addr: SocketAddr,
    pub run_id: RunId,
    pub auth_token: UserToken,
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
        let request = client
            .get(queue_api)
            .bearer_auth(api_key)
            .query(&[("run_id", run_id.to_string())]);
        let resp: HostedQueueResponse = send_request_with_decay(request)?
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
                UserToken::from_str(token_str)
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

// Decay with {5, 10, 20, 40, 80, 160}-second delays; eats ~ 5 minutes at most
const DEFAULT_REQUEST_ATTEMPTS: usize = 6;
const DEFAULT_REQUEST_DECAY: Duration = Duration::from_secs(5);
const DEFAULT_REQUEST_DECAY_MUL: u32 = 2;

fn build_retrier(
    max_attempts: usize,
    mut decay: Duration,
    decay_mul: u32,
) -> impl FnMut(usize) -> bool {
    move |last_attempt| {
        if last_attempt == max_attempts {
            return false;
        }
        thread::sleep(decay);
        decay *= decay_mul;
        true
    }
}

/// Attempts a request a number of times, retrying with an exponential decay if the server was
/// determined to have errored or notified a rate-limit.
fn send_request_with_decay(
    request: RequestBuilder,
) -> reqwest::Result<reqwest::blocking::Response> {
    send_request_with_decay_help(
        request,
        build_retrier(
            DEFAULT_REQUEST_ATTEMPTS,
            DEFAULT_REQUEST_DECAY,
            DEFAULT_REQUEST_DECAY_MUL,
        ),
    )
}

fn send_request_with_decay_help(
    request: RequestBuilder,
    mut wait_for_retry: impl FnMut(usize) -> bool,
) -> reqwest::Result<reqwest::blocking::Response> {
    let mut last_attempt = 0;
    loop {
        last_attempt += 1;

        let response = request
            .try_clone()
            .expect("bodies provided in the hosted API should never be streams")
            .send()?;

        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }

        if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
            if wait_for_retry(last_attempt) {
                tracing::debug!(?status, "server error, retrying after decay");
                continue;
            } else {
                tracing::info!(?last_attempt, "not retrying hosted API");
                return Ok(response);
            }
        } else {
            // There may be other errors in the response, but they are not something that we should
            // retry for.
            return Ok(response);
        }
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use abq_utils::{auth::UserToken, net_opt::Tls, net_protocol::workers::RunId};
    use reqwest::StatusCode;

    use crate::{send_request_with_decay_help, ApiKey};

    use super::{Error, HostedQueueConfig};

    use mockito::{mock, Matcher};

    fn test_api_key() -> ApiKey {
        ApiKey::from_str("abqapi_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF").unwrap()
    }

    fn test_auth_token() -> UserToken {
        UserToken::from_str("abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF").unwrap()
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

    #[test]
    fn retry_request_on_429() {
        let mut m = mock("GET", "/")
            .match_query(Matcher::Any)
            .with_status(429)
            .create();

        let mut last_seen_attempt = 0;

        let retrier = |last_attempt| {
            last_seen_attempt = last_attempt;
            if last_attempt == 1 {
                m = mock("GET", "/")
                    .match_query(Matcher::Any)
                    .with_status(200)
                    .create();
                true
            } else {
                false
            }
        };

        let client = reqwest::blocking::Client::new();
        let resp =
            send_request_with_decay_help(client.get(mockito::server_url()), retrier).unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(last_seen_attempt, 1);
    }

    #[test]
    fn retry_request_on_500() {
        let mut m = mock("GET", "/")
            .match_query(Matcher::Any)
            .with_status(500)
            .create();

        let mut last_seen_attempt = 0;

        let retrier = |last_attempt| {
            last_seen_attempt = last_attempt;
            if last_attempt == 1 {
                m = mock("GET", "/")
                    .match_query(Matcher::Any)
                    .with_status(200)
                    .create();
                true
            } else {
                false
            }
        };

        let client = reqwest::blocking::Client::new();
        let resp =
            send_request_with_decay_help(client.get(mockito::server_url()), retrier).unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(last_seen_attempt, 1);
    }

    #[test]
    fn do_not_retry_request_on_400_level() {
        let mut m = mock("GET", "/")
            .match_query(Matcher::Any)
            .with_status(401)
            .create();

        let mut last_seen_attempt = 0;

        let retrier = |last_attempt| {
            last_seen_attempt = last_attempt;
            if last_attempt == 1 {
                m = mock("GET", "/")
                    .match_query(Matcher::Any)
                    .with_status(200)
                    .create();
                true
            } else {
                false
            }
        };

        let client = reqwest::blocking::Client::new();
        let resp =
            send_request_with_decay_help(client.get(mockito::server_url()), retrier).unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(last_seen_attempt, 0);
    }

    #[test]
    fn do_not_retry_request_when_retrier_is_done() {
        let _m = mock("GET", "/")
            .match_query(Matcher::Any)
            .with_status(500)
            .create();

        let mut last_seen_attempt = 0;

        let retrier = |last_attempt| {
            last_seen_attempt = last_attempt;
            last_attempt < 3
        };

        let client = reqwest::blocking::Client::new();
        let resp =
            send_request_with_decay_help(client.get(mockito::server_url()), retrier).unwrap();

        assert_eq!(resp.status().as_u16(), 500);
        assert_eq!(last_seen_attempt, 3);
    }
}
