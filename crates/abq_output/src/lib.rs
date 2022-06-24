use std::fmt::Write;
use std::time::Duration;

use abq_utils::net_protocol::{
    runners::Output,
    workers::{WorkId, WorkerResult},
};

pub fn format_results(mut results: Vec<(WorkId, WorkerResult)>) -> String {
    results.sort_by(|(id1, _), (id_2, _)| id1.cmp(id_2));
    results
        .into_iter()
        .map(|(_id, result)| format_result(result))
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn format_result(result: WorkerResult) -> String {
    match result {
        WorkerResult::Output(Output { success, message }) => {
            if success {
                "OK".to_string()
            } else {
                format!("FAIL: {}", message)
            }
        }
        WorkerResult::Timeout(duration) => {
            format!("timeout after {}", format_duration(duration))
        }
        WorkerResult::Panic(msg) => format!("fatal error: {}", msg),
    }
}

fn format_duration(duration: Duration) -> String {
    let millis = duration.as_millis();
    let seconds = millis / 1000;
    let minutes = seconds / 60;

    let mut buf = String::new();
    if minutes > 0 {
        write!(&mut buf, "{} m", minutes).unwrap();
    }
    if seconds > 0 {
        if !buf.is_empty() {
            buf.push_str(", ");
        }
        write!(&mut buf, "{} s", seconds).unwrap();
    }
    if millis > 0 {
        if !buf.is_empty() {
            buf.push_str(", ");
        }
        write!(&mut buf, "{} ms", millis).unwrap();
    }

    buf
}
