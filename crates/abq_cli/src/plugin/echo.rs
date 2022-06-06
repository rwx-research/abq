use abq_output::format_result;
use abq_workers::protocol::{WorkId, WorkerAction};

use crate::collect::{CollectInputs, Collector};

#[derive(Default)]
pub struct Ctx {
    next_id: usize,
}

impl Ctx {
    fn next_id(&mut self) -> WorkId {
        self.next_id += 1;
        WorkId(self.next_id.to_string())
    }
}

pub fn collector(strings: Vec<String>) -> Collector<String, Ctx, impl CollectInputs<String>> {
    let collect_strings = || strings;

    let create_work = |ctx: &mut Ctx, s: String| {
        let id = ctx.next_id();
        let prefix = format!("echo {s}");
        (prefix, id, WorkerAction::Echo(s))
    };

    let report_result = format_result;

    Collector {
        _input: std::marker::PhantomData::default(),
        context: Ctx::default(),
        collect_message: "Discovering tests",
        collect_inputs: collect_strings,
        create_work,
        report_result,
    }
}
