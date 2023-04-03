use std::{fmt::Display, str::FromStr};

use abq_queue::persistence::remote::{CustomPersister, NoopPersister, RemotePersister};
#[cfg(feature = "s3")]
use abq_queue::persistence::remote::{S3Client, S3Persister};

use crate::args::Cli;

use clap::{error::ErrorKind, CommandFactory};

pub const ENV_REMOTE_PERSISTENCE_STRATEGY: &str = "ABQ_REMOTE_PERSISTENCE_STRATEGY";
pub const ENV_REMOTE_PERSISTENCE_COMMAND: &str = "ABQ_REMOTE_PERSISTENCE_COMMAND";
pub const ENV_REMOTE_PERSISTENCE_S3_BUCKET: &str = "ABQ_REMOTE_PERSISTENCE_S3_BUCKET";
pub const ENV_REMOTE_PERSISTENCE_S3_KEY_PREFIX: &str = "ABQ_REMOTE_PERSISTENCE_S3_KEY_PREFIX";

#[derive(Clone)]
pub enum RemotePersistenceStrategy {
    S3,
    Custom,
}

impl Display for RemotePersistenceStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RemotePersistenceStrategy::S3 => write!(f, "s3"),
            RemotePersistenceStrategy::Custom => write!(f, "custom"),
        }
    }
}

impl FromStr for RemotePersistenceStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "s3" => Ok(RemotePersistenceStrategy::S3),
            "custom" => Ok(RemotePersistenceStrategy::Custom),
            _ => Err(format!("unknown remote persistence strategy: {}", s)),
        }
    }
}

pub struct RemotePersistenceConfig {
    strategy: Option<RemotePersistenceStrategy>,
    remote_persistence_command: Option<String>,
    remote_persistence_s3_bucket: Option<String>,
    remote_persistence_s3_key_prefix: Option<String>,
}

pub type RemotePersisterResult = Result<RemotePersister, clap::Error>;

impl RemotePersistenceConfig {
    pub fn new(
        strategy: Option<RemotePersistenceStrategy>,
        remote_persistence_command: Option<String>,
        remote_persistence_s3_bucket: Option<String>,
        remote_persistence_s3_key_prefix: Option<String>,
    ) -> Self {
        Self {
            strategy,
            remote_persistence_command,
            remote_persistence_s3_bucket,
            remote_persistence_s3_key_prefix,
        }
    }

    pub async fn resolve(self) -> RemotePersisterResult {
        let Self {
            strategy,
            remote_persistence_command,
            remote_persistence_s3_bucket,
            remote_persistence_s3_key_prefix,
        } = self;

        let strategy = match strategy {
            Some(strategy) => strategy,
            None => return Ok(RemotePersister::new(NoopPersister::new())),
        };

        use RemotePersistenceStrategy::*;
        match strategy {
            S3 => {
                #[cfg(feature = "s3")]
                {
                    build_s3_strategy(
                        remote_persistence_s3_bucket,
                        remote_persistence_s3_key_prefix,
                    )
                    .await
                }

                #[cfg(not(feature = "s3"))]
                {
                    let mut cmd = Cli::command();
                    Err(cmd.error(
                        ErrorKind::InvalidValue,
                        format!(r#"Persistence to S3 is only available when compiled with the "s3" feature"#),
                    ))
                }
            }
            Custom => build_custom_strategy(remote_persistence_command),
        }
    }
}

fn missing_arg_for(arg: &str, strategy: RemotePersistenceStrategy) -> clap::Error {
    let mut cmd = Cli::command();
    cmd.error(
        ErrorKind::MissingRequiredArgument,
        format!(r#""{arg}" must be specified for remote persistence strategy "{strategy}""#),
    )
}

#[cfg(feature = "s3")]
async fn build_s3_strategy(
    remote_persistence_s3_bucket: Option<String>,
    remote_persistence_s3_key_prefix: Option<String>,
) -> RemotePersisterResult {
    let bucket = remote_persistence_s3_bucket.ok_or_else(|| {
        missing_arg_for(
            ENV_REMOTE_PERSISTENCE_S3_BUCKET,
            RemotePersistenceStrategy::S3,
        )
    })?;
    let key_prefix = remote_persistence_s3_key_prefix.ok_or_else(|| {
        missing_arg_for(
            ENV_REMOTE_PERSISTENCE_S3_KEY_PREFIX,
            RemotePersistenceStrategy::S3,
        )
    })?;

    let client = S3Client::new_from_env().await;

    let persister = S3Persister::new(client, bucket, key_prefix);

    Ok(RemotePersister::new(persister))
}

fn build_custom_strategy(remote_persistence_command: Option<String>) -> RemotePersisterResult {
    let command = remote_persistence_command.ok_or_else(|| {
        missing_arg_for(
            ENV_REMOTE_PERSISTENCE_COMMAND,
            RemotePersistenceStrategy::Custom,
        )
    })?;

    let mut args: Vec<_> = command.split(',').map(ToOwned::to_owned).collect();
    let command = args.remove(0);

    let persister = CustomPersister::new(command, args);

    Ok(RemotePersister::new(persister))
}
