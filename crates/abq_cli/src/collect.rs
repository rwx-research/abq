use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use abq_queue::invoke;
use abq_workers::protocol::{WorkId, WorkUnit, WorkerResult};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use crate::instance::AbqInstance;

fn spinner_style() -> ProgressStyle {
    ProgressStyle::default_spinner().template("{spinner} {prefix}{msg}")
}

fn spinner_progress_bar() -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(spinner_style());
    pb
}

/// Collects inputs for work to be requested by a collector.
pub trait CollectInputs<Input>: FnOnce() -> Vec<Input> {}
// Blanket impl CollectInputs for every closure of the same shape
impl<T, Input> CollectInputs<Input> for T where T: FnOnce() -> Vec<Input> {}

/// Creates a unit of work.
type CreateWork<Input, Ctx> = fn(&mut Ctx, Input) -> (String, WorkId, WorkUnit);

/// Formats a returned result as a string for reporting.
type ReportResult = fn(WorkerResult) -> String;

/// Describes how to collect dependencies for a task, segment them into units of work, and report
/// their results. Used by [run_work].
pub struct Collector<Input, Ctx, Collect>
where
    Collect: CollectInputs<Input>,
{
    pub _input: PhantomData<Input>,
    /// Opaque context used by the collector.
    pub context: Ctx,
    /// Interactive message to display while inputs are collected.
    pub collect_message: &'static str,

    pub collect_inputs: Collect,
    pub create_work: CreateWork<Input, Ctx>,
    pub report_result: ReportResult,
}

pub fn run_work<Input, Ctx, Collect: CollectInputs<Input>>(
    abq: AbqInstance,
    collector: Collector<Input, Ctx, Collect>,
) {
    let Collector {
        _input: _,
        collect_message,
        collect_inputs,
        mut context,
        create_work,
        report_result,
    } = collector;

    let progress_bar_timeout = Duration::from_secs(1) / 15;

    let inputs_progress = spinner_progress_bar();
    inputs_progress.set_message(collect_message);
    let (inputs_done_tx, inputs_done_rx) = mpsc::channel();
    let inputs_progress_handle = thread::spawn(move || loop {
        use mpsc::TryRecvError::*;
        match inputs_done_rx.try_recv() {
            Ok(()) => {
                inputs_progress.finish_and_clear();
                return;
            }
            Err(Empty) => {
                inputs_progress.inc(1);
                thread::sleep(progress_bar_timeout);
            }
            Err(Disconnected) => {
                panic!("channel is broken")
            }
        }
    });

    let inputs = collect_inputs();
    inputs_done_tx.send(()).unwrap();
    inputs_progress_handle.join().unwrap();

    let all_work_progress = MultiProgress::new();
    let num_work = inputs.len();

    let mut work = Vec::with_capacity(num_work);
    let mut progress_bars = HashMap::with_capacity(num_work);
    for input in inputs.into_iter() {
        let (prefix, id, action) = create_work(&mut context, input);
        work.push((id.clone(), action));

        let work_pb = all_work_progress.add(spinner_progress_bar());
        work_pb.set_prefix(prefix);
        let (work_done_tx, work_done_rx) = mpsc::channel();
        let work_pb_handle = thread::spawn(move || loop {
            use mpsc::TryRecvError::*;
            match work_done_rx.try_recv() {
                Ok(result) => {
                    let result_msg = report_result(result);
                    work_pb.finish_with_message(format!(": {}", result_msg));
                    return;
                }
                Err(Empty) => {
                    work_pb.inc(1);
                    thread::sleep(progress_bar_timeout);
                }
                Err(Disconnected) => panic!("channel is broken"),
            }
        });

        progress_bars.insert(id, (work_done_tx, work_pb_handle));
    }

    thread::spawn(move || all_work_progress.join());

    invoke::invoke_work(abq.socket(), work, |id, result| {
        let (tx_result, pb_handle) = progress_bars.remove(&id).unwrap();

        tx_result.send(result).unwrap();
        pb_handle.join().unwrap();
    });
}
