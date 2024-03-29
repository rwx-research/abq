use abq_utils::net_protocol::{runners::NativeRunnerSpecification, workers::RunId};
use reqwest::RequestBuilder;
use reqwest::Url;
use serde::Serialize;

use crate::{error::Error, AccessToken};

#[derive(Serialize)]
struct RecordTestRunNativeRunnerSpecification<'a> {
    name: &'a str,
    version: &'a str,
    host: &'a str,
}

#[derive(Serialize)]
struct RecordTestRunRequest<'a> {
    run_id: &'a str,
    native_runner_specification: RecordTestRunNativeRunnerSpecification<'a>,
}

pub async fn record_test_run_metadata<U>(
    api_url: U,
    access_token: &AccessToken,
    run_id: &RunId,
    native_runner_spec: &NativeRunnerSpecification,
) where
    U: AsRef<str>,
{
    const ATTEMPTS: usize = 2;

    // Swallow the error if we repeatedly fail to send the test run data, because there's not much
    // we can do to recover from that.
    let opt_error =
        record_test_run_metadata_help(api_url, access_token, run_id, native_runner_spec, ATTEMPTS)
            .await;
    if let Err(error) = opt_error {
        tracing::error!(attempts=?ATTEMPTS, ?error, "failed to send home test run metadata");
    }
}

async fn record_test_run_metadata_help<U>(
    api_url: U,
    access_token: &AccessToken,
    run_id: &RunId,
    native_runner_spec: &NativeRunnerSpecification,
    send_attempts: usize,
) -> Result<(), Error>
where
    U: AsRef<str>,
{
    // e.g. cloud.rwx.com/abq/api/record_test_run
    let queue_api = Url::try_from(api_url.as_ref())
        .map_err(|e| Error::InvalidUrl(e.to_string()))
        .and_then(|mut api| {
            // Really unfortunate API here. See https://github.com/servo/rust-url/issues/333;
            // perhaps it will improve in the future.
            api.path_segments_mut()
                .map_err(|()| Error::InvalidUrl(api_url.as_ref().to_string()))?
                .push("record_test_run");
            Ok(api)
        })?;

    let data = RecordTestRunRequest {
        run_id: &run_id.0,
        native_runner_specification: RecordTestRunNativeRunnerSpecification {
            name: &native_runner_spec.name,
            version: &native_runner_spec.version,
            host: &native_runner_spec.host,
        },
    };

    let client = reqwest::Client::new();
    let request = client
        .post(queue_api)
        .bearer_auth(access_token)
        .header("User-Agent", format!("abq/{}", abq_utils::VERSION))
        .json(&data);

    // Try sending the result at most twice.
    try_n_times(request, send_attempts)
        .await?
        .error_for_status()?;

    Ok(())
}

async fn try_n_times(request: RequestBuilder, tries: usize) -> reqwest::Result<reqwest::Response> {
    let mut i = 0;
    loop {
        i += 1;
        let response = request
            .try_clone()
            .expect("bodies provided in the hosted API should never be streams")
            .send()
            .await?;

        let status = response.status();
        if status.is_success() || i == tries {
            return Ok(response);
        }
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use abq_utils::net_protocol::{runners::NativeRunnerSpecification, workers::RunId};

    use mockito::{Matcher, Server};

    use crate::{error::Error, AccessToken};

    use super::record_test_run_metadata_help;

    fn test_access_token() -> AccessToken {
        AccessToken::from_str("abqapi_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF").unwrap()
    }

    fn test_native_runner_spec() -> NativeRunnerSpecification {
        NativeRunnerSpecification {
            name: "abq-runner".to_owned(),
            version: "1.2.3".to_owned(),
            test_framework: "runner-framework".to_owned(),
            test_framework_version: "4.5.6".to_owned(),
            language: "ocaml".to_owned(),
            language_version: "4.14".to_owned(),
            host: "ocaml 4.14 inhd".to_owned(),
        }
    }

    #[tokio::test]
    async fn send_success() {
        let run_id = RunId("1234".to_string());
        let mut server = Server::new();

        let _m = server.mock("POST", "/record_test_run")
            .match_header(
                "Authorization",
                format!("Bearer {}", test_access_token()).as_str(),
            )
            .match_header("User-Agent", format!("abq/{}", abq_utils::VERSION).as_str())
            .match_body(r#"{"run_id":"1234","native_runner_specification":{"name":"abq-runner","version":"1.2.3","host":"ocaml 4.14 inhd"}}"#)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body("")
            .create();

        let result = record_test_run_metadata_help(
            server.url(),
            &test_access_token(),
            &run_id,
            &test_native_runner_spec(),
            1,
        )
        .await;
        assert!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn get_hosted_queue_config_wrong_authn() {
        let mut server = Server::new();
        let _m = server
            .mock("POST", "/record_test_run")
            .match_query(Matcher::Any)
            .with_status(401)
            .create();

        let err = record_test_run_metadata_help(
            server.url(),
            &test_access_token(),
            &RunId("1234".to_owned()),
            &test_native_runner_spec(),
            1,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::Unauthenticated));
    }

    #[tokio::test]
    async fn get_hosted_queue_config_wrong_authz() {
        let mut server = Server::new();
        let _m = server
            .mock("POST", "/record_test_run")
            .match_query(Matcher::Any)
            .with_status(403)
            .create();

        let err = record_test_run_metadata_help(
            server.url(),
            &test_access_token(),
            &RunId("1234".to_owned()),
            &test_native_runner_spec(),
            1,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::Unauthorized));
    }

    #[tokio::test]
    async fn get_hosted_queue_config_bad_api_url() {
        let err = record_test_run_metadata_help(
            "definitely not a url",
            &test_access_token(),
            &RunId("1234".to_owned()),
            &test_native_runner_spec(),
            1,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::InvalidUrl(..)));
    }
}
