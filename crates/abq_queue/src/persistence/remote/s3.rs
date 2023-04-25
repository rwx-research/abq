use std::path::Path;

use abq_utils::{
    error::{OpaqueResult, ResultLocation},
    here,
    net_protocol::workers::RunId,
};
use async_trait::async_trait;
use aws_sdk_s3 as s3;
use s3::{
    error::SdkError,
    operation::{
        get_object::{GetObjectError, GetObjectOutput},
        head_object::{HeadObjectError, HeadObjectOutput},
        put_object::{PutObjectError, PutObjectOutput},
    },
    primitives::ByteStream,
};

use super::{PersistenceKind, RemotePersistence};

/// A representation of an AWS S3 client.
///
/// Creating a new S3 Client is expensive; prefer to clone a client multiple times instead, which
/// is cheap.
#[derive(Clone)]
pub struct S3Client(s3::Client);

impl S3Client {
    pub async fn new_from_env() -> Self {
        let sdk_config = aws_config::load_from_env().await;
        Self(s3::Client::new(&sdk_config))
    }
}

type PutResult = Result<PutObjectOutput, SdkError<PutObjectError>>;
type GetResult = Result<GetObjectOutput, SdkError<GetObjectError>>;
type HeadResult = Result<HeadObjectOutput, SdkError<HeadObjectError>>;

#[async_trait]
trait S3Impl {
    fn key_prefix(&self) -> &str;
    async fn put(&self, key: impl Into<String> + Send, body: ByteStream) -> PutResult;
    async fn get(&self, key: impl Into<String> + Send) -> GetResult;
    async fn head(&self, key: impl Into<String> + Send) -> HeadResult;
}

#[derive(Clone)]
pub struct S3Persister {
    client: s3::Client,
    bucket: String,
    key_prefix: String,
}

impl S3Persister {
    /// Initializes a new
    pub fn new(client: S3Client, bucket: impl Into<String>, key_prefix: impl Into<String>) -> Self {
        Self {
            client: client.0,
            bucket: bucket.into(),
            key_prefix: key_prefix.into(),
        }
    }
}

#[async_trait]
impl S3Impl for S3Persister {
    fn key_prefix(&self) -> &str {
        &self.key_prefix
    }

    async fn put(&self, key: impl Into<String> + Send, body: ByteStream) -> PutResult {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .send()
            .await
    }

    async fn get(&self, key: impl Into<String> + Send) -> GetResult {
        self.client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
    }

    async fn head(&self, key: impl Into<String> + Send) -> HeadResult {
        self.client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
    }
}

/// Builds a key for an S3 bucket of form
///
/// ```text
/// <prefix>/<run_id>/<kind>
/// ```
#[inline]
fn build_key(prefix: &str, persistence_kind: PersistenceKind, run_id: &RunId) -> String {
    let kind = persistence_kind.kind_str();
    let run_id = &run_id.0;
    let ext = persistence_kind.file_extension();
    format!("{prefix}/{run_id}/{kind}.{ext}")
}

#[async_trait]
impl<T> RemotePersistence for T
where
    T: S3Impl + Clone + Send + Sync + 'static,
{
    async fn store_from_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        from_local_path: &Path,
    ) -> OpaqueResult<()> {
        let key = build_key(self.key_prefix(), kind, run_id);
        let body = ByteStream::from_path(from_local_path)
            .await
            .located(here!())?;

        let _put_object = self.put(key, body).await.located(here!())?;

        Ok(())
    }

    async fn load_to_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        into_local_path: &Path,
    ) -> OpaqueResult<()> {
        let key = build_key(self.key_prefix(), kind, run_id);
        let get_object = self.get(key).await.located(here!())?;

        let mut body = get_object.body.into_async_read();
        let mut file = tokio::fs::File::create(into_local_path)
            .await
            .located(here!())?;
        tokio::io::copy(&mut body, &mut file)
            .await
            .located(here!())?;

        Ok(())
    }

    async fn has_run_id(&self, run_id: &RunId) -> OpaqueResult<bool> {
        let key = build_key(self.key_prefix(), PersistenceKind::Manifest, run_id);
        let result = self.head(key).await;
        match result {
            Ok(_) => Ok(true),
            Err(e) => match e {
                SdkError::ServiceError(e) if matches!(e.err(), HeadObjectError::NotFound(_)) => {
                    Ok(false)
                }
                _ => Err(e).located(here!()),
            },
        }
    }

    fn boxed_clone(&self) -> Box<dyn RemotePersistence + Send + Sync> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod fake {
    use async_trait::async_trait;
    use aws_sdk_s3::primitives::ByteStream;
    use tokio::io::AsyncReadExt;

    use super::{GetResult, HeadResult, PutResult, S3Impl};

    #[derive(Clone)]
    pub struct S3Fake<OnPut, OnGet, OnHead> {
        key_prefix: String,
        on_put: OnPut,
        on_get: OnGet,
        on_head: OnHead,
    }

    impl<OnPut, OnGet, OnHead> S3Fake<OnPut, OnGet, OnHead>
    where
        OnPut: Fn(String, &[u8]) -> PutResult + Send + Sync,
        OnGet: Fn(String) -> GetResult + Send + Sync,
        OnHead: Fn(String) -> HeadResult + Send + Sync,
    {
        pub fn new(
            key_prefix: impl Into<String>,
            on_put: OnPut,
            on_get: OnGet,
            on_head: OnHead,
        ) -> Self {
            Self {
                key_prefix: key_prefix.into(),
                on_put,
                on_get,
                on_head,
            }
        }
    }

    #[async_trait]
    impl<OnPut, OnGet, OnHead> S3Impl for S3Fake<OnPut, OnGet, OnHead>
    where
        OnPut: Fn(String, &[u8]) -> PutResult + Send + Sync,
        OnGet: Fn(String) -> GetResult + Send + Sync,
        OnHead: Fn(String) -> HeadResult + Send + Sync,
    {
        fn key_prefix(&self) -> &str {
            &self.key_prefix
        }

        async fn put(&self, key: impl Into<String> + Send, body: ByteStream) -> PutResult {
            let mut buf = vec![];
            let mut body = body.into_async_read();
            body.read_to_end(&mut buf).await.unwrap();
            (self.on_put)(key.into(), &buf)
        }

        async fn get(&self, key: impl Into<String> + Send) -> GetResult {
            (self.on_get)(key.into())
        }

        async fn head(&self, key: impl Into<String> + Send) -> HeadResult {
            (self.on_head)(key.into())
        }
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use super::fake::S3Fake;
    use super::{build_key, PersistenceKind};
    use crate::persistence::remote::RemotePersistence;
    use abq_utils::net_protocol::workers::RunId;
    use aws_sdk_s3::error::SdkError;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::operation::head_object::builders::HeadObjectOutputBuilder;
    use aws_sdk_s3::operation::head_object::HeadObjectError;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_sdk_s3::primitives::{ByteStream, SdkBody};
    use aws_sdk_s3::types::error::NotFound;
    use aws_smithy_http::operation::Response;
    use aws_smithy_http::result::ServiceError;
    use tempfile::NamedTempFile;

    #[test]
    fn test_build_key_results() {
        let run_id = RunId("test-run-id".to_owned());
        let kind = PersistenceKind::Results;
        let prefix = "test-prefix";

        let key = build_key(prefix, kind, &run_id);
        assert_eq!(key, "test-prefix/test-run-id/results.jsonl");
    }

    #[test]
    fn test_build_key_manifest() {
        let run_id = RunId("test-run-id".to_owned());
        let kind = PersistenceKind::Manifest;
        let prefix = "test-prefix";

        let key = build_key(prefix, kind, &run_id);
        assert_eq!(key, "test-prefix/test-run-id/manifest.json");
    }

    #[tokio::test]
    async fn store_from_disk_okay() {
        let s3 = S3Fake::new(
            "bucket-prefix",
            |key, body| {
                assert_eq!(key, "bucket-prefix/test-run-id/manifest.json");
                assert_eq!(body, b"manifest-body");
                Ok(PutObjectOutput::builder().build())
            },
            |_| unreachable!(),
            |_| unreachable!(),
        );

        let mut manifest = NamedTempFile::new().unwrap();
        io::Write::write_all(&mut manifest, b"manifest-body").unwrap();

        s3.store_from_disk(
            PersistenceKind::Manifest,
            &RunId("test-run-id".to_owned()),
            manifest.path(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn load_okay() {
        let s3 = S3Fake::new(
            "bucket-prefix",
            |_key, _body| unreachable!(),
            |key| {
                assert_eq!(key, "bucket-prefix/test-run-id/manifest.json");
                Ok(GetObjectOutput::builder()
                    .body(ByteStream::from(b"manifest-body".to_vec()))
                    .build())
            },
            |_| unreachable!(),
        );

        let manifest = NamedTempFile::new().unwrap();

        s3.load_to_disk(
            PersistenceKind::Manifest,
            &RunId("test-run-id".to_owned()),
            manifest.path(),
        )
        .await
        .unwrap();

        let mut buf = vec![];
        io::Read::read_to_end(&mut io::BufReader::new(manifest), &mut buf).unwrap();

        assert_eq!(buf, b"manifest-body");
    }

    #[tokio::test]
    async fn store_from_disk_error() {
        let s3 = S3Fake::new(
            "bucket-prefix",
            |key, body| {
                assert_eq!(key, "bucket-prefix/test-run-id/manifest.json");
                assert_eq!(body, b"manifest-body");
                Err(SdkError::timeout_error("timed out"))
            },
            |_| unreachable!(),
            |_| unreachable!(),
        );

        let mut manifest = NamedTempFile::new().unwrap();
        io::Write::write_all(&mut manifest, b"manifest-body").unwrap();

        let err = s3
            .store_from_disk(
                PersistenceKind::Manifest,
                &RunId("test-run-id".to_owned()),
                manifest.path(),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn load_error() {
        let s3 = S3Fake::new(
            "bucket-prefix",
            |_key, _body| unreachable!(),
            |key| {
                assert_eq!(key, "bucket-prefix/test-run-id/manifest.json");
                Err(SdkError::timeout_error("timed out"))
            },
            |_| unreachable!(),
        );

        let manifest = NamedTempFile::new().unwrap();

        let err = s3
            .load_to_disk(
                PersistenceKind::Manifest,
                &RunId("test-run-id".to_owned()),
                manifest.path(),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn run_id_exists_yes() {
        let s3 = S3Fake::new(
            "bucket-prefix",
            |_key, _body| unreachable!(),
            |_key| unreachable!(),
            |_key| Ok(HeadObjectOutputBuilder::default().build()),
        );

        let exists = s3
            .has_run_id(&RunId("test-run-id".to_owned()))
            .await
            .unwrap();

        assert!(exists);
    }

    #[tokio::test]
    async fn run_id_exists_no() {
        let s3 = S3Fake::new(
            "bucket-prefix",
            |_key, _body| unreachable!(),
            |_key| unreachable!(),
            |_key| {
                Err(SdkError::ServiceError(
                    ServiceError::builder()
                        .source(HeadObjectError::NotFound(NotFound::builder().build()))
                        .raw(Response::new(http::response::Response::new(
                            SdkBody::empty(),
                        )))
                        .build(),
                ))
            },
        );

        let exists = s3
            .has_run_id(&RunId("test-run-id".to_owned()))
            .await
            .unwrap();

        assert!(!exists);
    }

    #[tokio::test]
    async fn run_id_exists_error() {
        let s3 = S3Fake::new(
            "bucket-prefix",
            |_key, _body| unreachable!(),
            |_key| unreachable!(),
            |key| {
                assert_eq!(key, "bucket-prefix/test-run-id/manifest.json");
                Err(SdkError::timeout_error("timed out"))
            },
        );

        let err = s3
            .has_run_id(&RunId("test-run-id".to_owned()))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("timed out"));
    }
}
