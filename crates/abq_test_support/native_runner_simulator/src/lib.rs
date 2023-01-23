use abq_utils::net_protocol::{self, runners::ABQ_GENERATE_MANIFEST};
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    io::{self, Write},
    process,
};
use tokio::net::TcpStream;

#[derive(Serialize, Deserialize, Debug)]
pub enum Msg {
    Connect,
    /// Opaquely write a protocol message that's already been serialized to a json value
    OpaqueWrite(serde_json::Value),
    /// Opaquely read a json protocol message
    OpaqueRead,
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    IfGenerateManifest {
        then_do: Vec<Msg>,
        else_do: Vec<Msg>,
    },
    Exit(i32),
}

pub fn pack<S: serde::Serialize>(v: S) -> serde_json::Value {
    serde_json::to_value(v).unwrap()
}

pub fn pack_msgs(msgs: impl IntoIterator<Item = Msg>) -> String {
    let msgs: Vec<Msg> = msgs.into_iter().collect();
    serde_json::to_string(&msgs).unwrap()
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
            Msg::Exit(ec) => {
                process::exit(ec);
            }
        }
    }
}
