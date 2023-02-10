use abq_utils::{
    exit::ExitCode,
    net_protocol::{self, entity::Entity, queue, workers::RunId},
};
use anyhow::anyhow;

use crate::instance::AbqInstance;

/// Returns true iff ABQ should determine the test run exit code itself.
pub(crate) fn should_track_exit_code_in_band() -> bool {
    if matches!(std::env::var("ABQ_SET_EXIT_CODE").as_deref(), Ok("false")) {
        return false;
    }

    true
}

pub(crate) fn set_exit_code(
    entity: Entity,
    abq: AbqInstance,
    run_id: RunId,
    exit_code: ExitCode,
) -> anyhow::Result<()> {
    let queue_addr = abq.server_addr();
    let client = abq.take_client_options().build()?;
    let mut conn = client.connect(queue_addr)?;

    let request = queue::Request {
        entity,
        message: queue::Message::SetOutOfBandExitCode(run_id, exit_code),
    };

    net_protocol::write(&mut conn, request)?;
    let response: queue::SetOutOfBandExitCodeResponse = net_protocol::read(&mut conn)?;

    let failure_reason = match response {
        queue::SetOutOfBandExitCodeResponse::Success => return Ok(()),
        queue::SetOutOfBandExitCodeResponse::Failure(reason) => reason,
    };

    use queue::CannotSetOOBExitCodeReason::*;
    let failure_prefix = "failed to set the ABQ run exit code";
    let failure_message = match failure_reason {
        RunIsActive => format!("{failure_prefix}, because the run is currently still in progress"),
        ExitCodeAlreadyDetermined => {
            format!("{failure_prefix}, because the run already had an exit code set for it")
        }
        RunWasCancelled { reason: _ } => format!("{failure_prefix}, because the run was cancelled"),
    };

    Err(anyhow!(failure_message))
}
