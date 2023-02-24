use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use abq_utils::net_protocol::workers::RunId;
use serde_derive::Serialize;

#[derive(Serialize)]
struct WorkerState<'a> {
    abq_executable: &'a Path,
    abq_version: &'a str,
    run_id: &'a RunId,
}

/// Optionally writes a statefile for an ABQ worker invocation, if the invocation state indicates so.
///
/// Intended for integration with Captain. See RWX RFC#18.
pub(crate) fn optional_write_worker_statefile(run_id: &RunId) -> anyhow::Result<()> {
    let statefile = match std::env::var("ABQ_STATE_FILE") {
        Ok(path_s) => PathBuf::from(path_s),
        Err(_) => {
            // statefile var not present, don't write it.
            return Ok(());
        }
    };

    let state = WorkerState {
        abq_executable: &std::env::current_exe()?,
        abq_version: abq_utils::VERSION,
        run_id,
    };

    let mut statefile = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(statefile)?;

    serde_json::to_writer(&statefile, &state)?;
    statefile.flush()?;

    Ok(())
}
