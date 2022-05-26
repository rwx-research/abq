use std::path::Path;
use std::{os::unix::net::UnixStream, time::Duration};

use crate::protocol::{Message, Shutdown, WorkItem, WorkResult};

pub fn send_work(socket: &Path, work: WorkItem) {
    let stream = UnixStream::connect(socket).expect("socket not available");
    serde_json::to_writer(stream, &Message::Work(work)).unwrap();
}

pub(crate) fn send_result(socket: &Path, result: WorkResult) {
    let stream = UnixStream::connect(socket).expect("socket not available");
    serde_json::to_writer(stream, &Message::Result(result)).unwrap();
}

pub(crate) fn send_shutdown(socket: &Path, opt_expect_n: Option<usize>, timeout: Duration) {
    let stream = UnixStream::connect(socket).expect("socket not available");
    serde_json::to_writer(
        stream,
        &Message::Shutdown(Shutdown {
            timeout,
            opt_expect_n,
        }),
    )
    .unwrap();
}
