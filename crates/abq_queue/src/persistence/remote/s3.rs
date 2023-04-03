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

type PutResult = Result<PutObjectOutput, SdkError<PutObjectError>>;
type GetResult = Result<GetObjectOutput, SdkError<GetObjectError>>;

#[async_trait]
trait S3Impl {
    fn key_prefix(&self) -> &str;
    async fn put(&self, key: impl Into<String> + Send, body: ByteStream) -> PutResult;
    async fn get(&self, key: impl Into<String> + Send) -> GetResult;
}

#[derive(Clone)]
pub struct S3Persister {
    client: s3::Client,
    bucket: String,
    key_prefix: String,
}

impl S3Persister {
    /// Initializes a new
    pub async fn new(
        client: S3Client,
        bucket: impl Into<String>,
        key_prefix: impl Into<String>,
    ) -> Self {
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
}

/// Builds a key for an S3 bucket of form
///
/// ```
/// <prefix>/<run_id>/<kind>
/// ```
#[inline]
fn build_key(prefix: &str, kind: PersistenceKind, run_id: RunId) -> impl Into<String> {
    let kind_str = match kind {
        PersistenceKind::Manifest => "manifest",
        PersistenceKind::Results => "results",
    };
    [&prefix, run_id.0.as_str(), kind_str].join("/")
}

#[async_trait]
impl<T> RemotePersistence for T
where
    T: S3Impl + Send + Sync,
{
    async fn store(
        &self,
        kind: PersistenceKind,
        run_id: RunId,
        from_local_path: &Path,
    ) -> OpaqueResult<()> {
        let key = build_key(self.key_prefix(), kind, run_id);
        let body = ByteStream::from_path(from_local_path)
            .await
            .located(here!())?;

        let _put_object = self.put(key, body).await.located(here!())?;

        Ok(())
    }

    async fn load(
        &self,
        kind: PersistenceKind,
        run_id: RunId,
        into_local_path: &Path,
    ) -> OpaqueResult<()> {
        let key = build_key(&self.key_prefix(), kind, run_id);
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
}

#[cfg(test)]
mod fake {

    use async_trait::async_trait;
    use aws_sdk_s3::primitives::ByteStream;
    use tokio::io::AsyncReadExt;

    use super::{GetResult, PutResult, S3Impl};

    pub struct S3Fake<OnPut, OnGet> {
        key_prefix: String,
        on_put: OnPut,
        on_get: OnGet,
    }

    impl<OnPut, OnGet> S3Fake<OnPut, OnGet>
    where
        OnPut: Fn(String, &[u8]) -> PutResult + Send + Sync,
        OnGet: Fn(String) -> GetResult + Send + Sync,
    {
        pub fn new(key_prefix: impl Into<String>, on_put: OnPut, on_get: OnGet) -> Self {
            Self {
                key_prefix: key_prefix.into(),
                on_put,
                on_get,
            }
        }
    }

    #[async_trait]
    impl<OnPut, OnGet> S3Impl for S3Fake<OnPut, OnGet>
    where
        OnPut: Fn(String, &[u8]) -> PutResult + Send + Sync,
        OnGet: Fn(String) -> GetResult + Send + Sync,
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
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use super::fake::S3Fake;
    use super::{build_key, PersistenceKind};
    use crate::persistence::remote::RemotePersistence;
    use abq_utils::net_protocol::workers::RunId;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_sdk_s3::primitives::ByteStream;
    use tempfile::NamedTempFile;

    #[test]
    fn test_build_key_results() {
        let run_id = RunId("test-run-id".to_owned());
        let kind = PersistenceKind::Results;
        let prefix = "test-prefix";

        let key = build_key(prefix, kind, run_id);
        assert_eq!(key.into(), "test-prefix/test-run-id/results");
    }

    #[test]
    fn test_build_key_manifest() {
        let run_id = RunId("test-run-id".to_owned());
        let kind = PersistenceKind::Manifest;
        let prefix = "test-prefix";

        let key = build_key(prefix, kind, run_id);
        assert_eq!(key.into(), "test-prefix/test-run-id/manifest");
    }

    #[tokio::test]
    async fn store_okay() {
        let s3 = S3Fake::new(
            "bucket-prefix",
            |key, body| {
                assert_eq!(key, "bucket-prefix/test-run-id/manifest");
                assert_eq!(body, b"manifest-body");
                Ok(PutObjectOutput::builder().build())
            },
            |_| unreachable!(),
        );

        let mut manifest = NamedTempFile::new().unwrap();
        io::Write::write_all(&mut manifest, b"manifest-body").unwrap();

        s3.store(
            PersistenceKind::Manifest,
            RunId("test-run-id".to_owned()),
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
                assert_eq!(key, "bucket-prefix/test-run-id/manifest");
                Ok(GetObjectOutput::builder()
                    .body(ByteStream::from(b"manifest-body".to_vec()))
                    .build())
            },
        );

        let manifest = NamedTempFile::new().unwrap();

        s3.load(
            PersistenceKind::Manifest,
            RunId("test-run-id".to_owned()),
            manifest.path(),
        )
        .await
        .unwrap();

        let mut buf = vec![];
        io::Read::read_to_end(&mut io::BufReader::new(manifest), &mut buf).unwrap();

        assert_eq!(buf, b"manifest-body");
    }
}
