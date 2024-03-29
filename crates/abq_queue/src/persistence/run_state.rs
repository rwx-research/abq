//! Representation of run state that can be persisted to a remote and loaded from a remote.
//! See [crate::queue::RunState::InitialManifestDone].

use std::io;

use abq_utils::{
    error::OpaqueResult,
    exit::ExitCode,
    net_protocol::{entity::Entity, runners::MetadataMap, workers::RunId},
    test_command_hash::TestCommandHash,
};
use serde_derive::{Deserialize, Serialize};
use thiserror::Error;

use super::remote::RemotePersister;

const CURRENT_SCHEMA_VERSION: u32 = 1;

/// The version of the schema used by this serialized state.
/// Incompatible schemas cannot be read.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
struct SchemaVersion {
    schema_version: u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct RunState {
    pub new_worker_exit_code: ExitCode,
    pub init_metadata: MetadataMap,
    pub seen_workers: Vec<Entity>,
    /// NB: Optional because this did not exist prior to ABQ 1.6.2.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub test_command_hash: Option<TestCommandHash>,
}

impl RunState {
    #[cfg(test)]
    pub fn fake() -> RunState {
        RunState {
            new_worker_exit_code: ExitCode::SUCCESS,
            init_metadata: MetadataMap::new(),
            seen_workers: Vec::new(),
            test_command_hash: Some(TestCommandHash::random()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct SerializedRunStateInner {
    #[serde(flatten)]
    schema_version: SchemaVersion,

    #[serde(flatten)]
    run_state: RunState,
}

/// Internal representation of a run state, that can be serialized to and deserialized from a
/// remote.
#[derive(Debug, PartialEq, Eq, Clone)]
#[repr(transparent)]
pub struct SerializableRunState(SerializedRunStateInner);

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("expected schema version {expected}, found {found}")]
    IncompatibleSchemaVersion { found: u32, expected: u32 },
    #[error("{0}")]
    Other(#[from] serde_json::Error),
}

impl SerializableRunState {
    pub fn new(run_state: RunState) -> Self {
        Self(SerializedRunStateInner {
            schema_version: SchemaVersion {
                schema_version: CURRENT_SCHEMA_VERSION,
            },
            run_state,
        })
    }

    pub fn into_run_state(self) -> RunState {
        self.0.run_state
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, ParseError> {
        let err = match serde_json::from_slice(bytes) {
            Ok(run_state) => return Ok(Self(run_state)),
            Err(err) => err,
        };

        // For a better error message, try to deserialize the schema version separately.
        let schema_version: SchemaVersion = serde_json::from_slice(bytes)?;
        if schema_version.schema_version != CURRENT_SCHEMA_VERSION {
            Err(ParseError::IncompatibleSchemaVersion {
                found: schema_version.schema_version,
                expected: CURRENT_SCHEMA_VERSION,
            })
        } else {
            Err(err.into())
        }
    }

    pub fn serialize_to(&self, writer: &mut impl io::Write) -> serde_json::Result<()> {
        serde_json::to_writer(writer, &self.0)
    }

    pub fn serialize(&self) -> serde_json::Result<Vec<u8>> {
        let mut writer = Vec::new();
        self.serialize_to(&mut writer)?;
        Ok(writer)
    }
}

#[derive(Debug)]
pub struct PersistRunStatePlan<'a> {
    run_id: &'a RunId,
    run_state: SerializableRunState,
}

impl<'a> PersistRunStatePlan<'a> {
    pub fn new(run_id: &'a RunId, run_state: RunState) -> Self {
        Self {
            run_id,
            run_state: SerializableRunState::new(run_state),
        }
    }

    pub async fn persist(self, remote: &RemotePersister) -> OpaqueResult<()> {
        remote.store_run_state(self.run_id, self.run_state).await
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_serialize_deserialize() {
        let run_state = RunState {
            new_worker_exit_code: ExitCode::SUCCESS,
            init_metadata: MetadataMap::new(),
            seen_workers: vec![],
            test_command_hash: Some(TestCommandHash::from_command("yarn", &["jest".to_owned()])),
        };
        let serialized = SerializableRunState::new(run_state.clone());
        let serialized_bytes = serialized.serialize().unwrap();
        let deserialized = SerializableRunState::deserialize(&serialized_bytes).unwrap();
        assert_eq!(deserialized.into_run_state(), run_state);

        insta::assert_json_snapshot!(serialized.0, @r###"
        {
          "schema_version": 1,
          "new_worker_exit_code": 0,
          "init_metadata": {},
          "seen_workers": [],
          "test_command_hash": [
            115,
            226,
            225,
            160,
            14,
            172,
            165,
            205,
            140,
            100,
            188,
            76,
            56,
            80,
            143,
            200,
            177,
            239,
            232,
            249,
            38,
            80,
            127,
            129,
            4,
            157,
            230,
            41,
            123,
            72,
            130,
            80
          ]
        }
        "###);
    }

    #[test]
    fn test_deserialize_incompatible_schema() {
        let serialized = r###"
        {
            "schema_version": 16,
            "worker_foobar": {}
        }
        "###;
        let err = SerializableRunState::deserialize(serialized.as_bytes()).unwrap_err();

        assert!(matches!(
            err,
            ParseError::IncompatibleSchemaVersion {
                expected: 1,
                found: 16
            }
        ));
    }

    #[test]
    fn test_deserialize_missing_test_command_hash() {
        let serialized = r###"
        {
          "schema_version": 1,
          "new_worker_exit_code": 0,
          "init_metadata": {},
          "seen_workers": []
        }
        "###;
        let result = SerializableRunState::deserialize(serialized.as_bytes()).unwrap();
        let result = result.into_run_state();

        assert_eq!(
            result,
            RunState {
                new_worker_exit_code: ExitCode::SUCCESS,
                init_metadata: MetadataMap::new(),
                seen_workers: vec![],
                test_command_hash: None,
            }
        );
    }

    #[test]
    fn test_deserialize_compatible_schema_but_different_version() {
        let run_state = RunState {
            new_worker_exit_code: ExitCode::SUCCESS,
            init_metadata: MetadataMap::new(),
            seen_workers: vec![],
            test_command_hash: Some(TestCommandHash::random()),
        };
        let serialized = SerializableRunState(SerializedRunStateInner {
            schema_version: SchemaVersion {
                schema_version: CURRENT_SCHEMA_VERSION + 10,
            },
            run_state: run_state.clone(),
        });
        let serialized_bytes = serialized.serialize().unwrap();
        let deserialized = SerializableRunState::deserialize(&serialized_bytes).unwrap();
        assert_eq!(deserialized.into_run_state(), run_state);
    }

    #[test]
    fn test_deserialize_bad_json() {
        let serialized = r###"
        {
            "schema_version": 16,
        "###;
        let err = SerializableRunState::deserialize(serialized.as_bytes()).unwrap_err();

        assert!(matches!(err, ParseError::Other(..)));
    }
}
