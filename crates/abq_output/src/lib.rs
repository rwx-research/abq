use std::fmt::Write;
use std::time::Duration;

use abq_utils::net_protocol::runners::{Status, TestResult};
use abq_utils::net_protocol::workers::WorkId;

pub fn format_results(mut results: Vec<(WorkId, TestResult)>) -> String {
    results.sort_by(|(id1, _), (id_2, _)| id1.cmp(id_2));
    results
        .into_iter()
        .map(|(_id, result)| format_result(result))
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn format_result(result: TestResult) -> String {
    let mut buf = String::new();
    buf.push_str(&result.display_name);
    buf.push_str(": ");
    buf.push_str(format_status(result.status));
    buf
}

fn format_status(status: Status) -> &'static str {
    match status {
        Status::Failure => "failure",
        Status::Success => "success",
        Status::Error => "error",
        Status::Pending => "pending",
        Status::Skipped => "skipped",
    }
}

#[allow(unused)]
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
