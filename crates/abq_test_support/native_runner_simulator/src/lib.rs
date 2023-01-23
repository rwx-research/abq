use abq_utils::net_protocol;
use serde_derive::{Deserialize, Serialize};
use std::{
    io::{self, Write},
    net::TcpStream,
    process,
};

#[derive(Serialize, Deserialize, Debug)]
pub enum Msg {
    Connect,
    /// Opaquely write a protocol message that's already been serialized to a json value
    OpaqueWrite(serde_json::Value),
    /// Opaquely read a json protocol message
    OpaqueRead,
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
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

pub fn run_simulation(msgs: impl IntoIterator<Item = Msg>) {
    let mut conn = None;
    macro_rules! conn {
        () => {
            conn.as_mut().unwrap()
        };
    }

    for msg in msgs.into_iter() {
        match msg {
            Msg::Connect => {
                let addr = std::env::var("ABQ_SOCKET").unwrap();
                conn = Some(TcpStream::connect(addr).unwrap());
            }
            Msg::OpaqueWrite(v) => {
                net_protocol::write(&mut conn!(), v).unwrap();
            }
            Msg::OpaqueRead => {
                net_protocol::read::<serde_json::Value>(&mut conn!()).unwrap();
            }
            Msg::Stdout(bytes) => {
                io::stdout().write_all(&bytes).unwrap();
                io::stdout().flush().unwrap();
            }
            Msg::Stderr(bytes) => {
                io::stderr().write_all(&bytes).unwrap();
                io::stderr().flush().unwrap();
            }
            Msg::Exit(ec) => {
                process::exit(ec);
            }
        }
    }
}
