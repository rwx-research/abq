use abq_utils::net_protocol::{
    self,
    entity::RunnerMeta,
    runners::{
        AbqProtocolVersion, NativeRunnerSpecification, ProtocolWitness,
        RawNativeRunnerSpawnedMessage, RawTestResultMessage, Status, TestCaseMessage, TestResult,
        TestResultSpec, ABQ_GENERATE_MANIFEST,
    },
};
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    io::{self, Write},
    path::PathBuf,
    process,
    time::Duration,
};
use tempfile::TempPath;
use tokio::{io::AsyncWriteExt, net::TcpStream};

pub fn legal_spawned_message(proto: ProtocolWitness) -> RawNativeRunnerSpawnedMessage {
    let protocol_version = proto.get_version();
    let runner_specification = NativeRunnerSpecification {
        name: "test".to_string(),
        version: "0.0.0".to_string(),
        test_framework: "rspec".to_owned(),
        test_framework_version: "3.12.0".to_owned(),
        language: "ruby".to_owned(),
        language_version: "3.1.2p20".to_owned(),
        host: "ruby 3.1.2p20 (2022-04-12 revision 4491bb740a) [x86_64-darwin21]".to_owned(),
    };
    RawNativeRunnerSpawnedMessage::new(proto, protocol_version, runner_specification)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Msg {
    Connect,
    /// Opaquely write a protocol message that's already been serialized to a json value
    OpaqueWrite(serde_json::Value),
    /// Opaquely read a json protocol message
    OpaqueRead,
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Sleep(Duration),
    IfGenerateManifest {
        then_do: Vec<Msg>,
        else_do: Vec<Msg>,
    },
    IfOpaqueReadSocketAlive {
        then_do: Vec<Msg>,
        else_do: Vec<Msg>,
    },
    /// Read a test message and write a fake result back.
    IfAliveReadAndWriteFake(Status),
    Exit(i32),
}

pub fn pack<S: serde::Serialize>(v: S) -> serde_json::Value {
    serde_json::to_value(v).unwrap()
}

pub fn pack_msgs(msgs: impl IntoIterator<Item = Msg>) -> String {
    let msgs: Vec<Msg> = msgs.into_iter().collect();
    serde_json::to_string(&msgs).unwrap()
}

pub struct PackedFile {
    _tmpfile: TempPath,
    pub path: PathBuf,
}

pub fn pack_msgs_to_disk(msgs: impl IntoIterator<Item = Msg>) -> PackedFile {
    let simulation_msg = pack_msgs(msgs);
    let simfile = tempfile::NamedTempFile::new().unwrap().into_temp_path();
    let simfile_path = simfile.to_path_buf();
    std::fs::write(&simfile_path, simulation_msg).unwrap();
    PackedFile {
        _tmpfile: simfile,
        path: simfile_path,
    }
}

pub fn read_msgs(pack: &str) -> Vec<Msg> {
    serde_json::from_str(pack).unwrap()
}

pub async fn run_simulation(msgs: impl IntoIterator<Item = Msg>) {
    let mut conn = None;
    macro_rules! conn {
        () => {
            conn.as_mut().unwrap()
        };
    }

    let mut queue = msgs.into_iter().collect::<VecDeque<_>>();

    while let Some(msg) = queue.pop_front() {
        match msg {
            Msg::Connect => {
                let addr = std::env::var("ABQ_SOCKET").unwrap();
                conn = Some(TcpStream::connect(addr).await.unwrap());
            }
            Msg::OpaqueWrite(v) => {
                net_protocol::async_write_local(&mut conn!(), &v)
                    .await
                    .unwrap();
            }
            Msg::OpaqueRead => {
                net_protocol::async_read_local::<_, serde_json::Value>(&mut conn!())
                    .await
                    .unwrap();
            }
            Msg::Stdout(bytes) => {
                io::stdout().write_all(&bytes).unwrap();
                io::stdout().flush().unwrap();
            }
            Msg::Stderr(bytes) => {
                io::stderr().write_all(&bytes).unwrap();
                io::stderr().flush().unwrap();
            }
            Msg::Sleep(duration) => {
                tokio::time::sleep(duration).await;
            }
            Msg::IfGenerateManifest { then_do, else_do } => {
                let extension = if std::env::var(ABQ_GENERATE_MANIFEST).as_deref() == Ok("1") {
                    then_do
                } else {
                    else_do
                };
                extension
                    .into_iter()
                    .rev()
                    .for_each(|action| queue.push_front(action));
            }
            Msg::IfOpaqueReadSocketAlive { then_do, else_do } => {
                let read_result =
                    net_protocol::async_read_local::<_, serde_json::Value>(&mut conn!()).await;
                let extension = if read_result.is_ok() {
                    then_do
                } else {
                    else_do
                };
                extension
                    .into_iter()
                    .rev()
                    .for_each(|action| queue.push_front(action));
            }
            Msg::IfAliveReadAndWriteFake(status) => {
                let read_result =
                    net_protocol::async_read_local::<_, TestCaseMessage>(&mut conn!()).await;
                if let Ok(tc) = read_result {
                    let id = tc.id();
                    let fake_result = RawTestResultMessage::from_test_result(
                        AbqProtocolVersion::V0_2.get_supported_witness().unwrap(),
                        TestResult::new(
                            RunnerMeta::fake(),
                            TestResultSpec {
                                id,
                                status,
                                ..TestResultSpec::fake()
                            },
                        ),
                    );

                    net_protocol::async_write_local(&mut conn!(), &fake_result)
                        .await
                        .unwrap();
                };
            }
            Msg::Exit(ec) => {
                io::stdout().flush().unwrap();
                io::stderr().flush().unwrap();
                let _ = conn!().flush().await;
                let _ = conn!().shutdown().await;
                process::exit(ec);
            }
        }
    }
}
