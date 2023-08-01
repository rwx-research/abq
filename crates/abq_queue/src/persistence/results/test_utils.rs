#![cfg(test)]

use std::sync::Arc;

use abq_utils::{
    error::{ErrorLocation, OpaqueResult, ResultLocation},
    here,
    net_protocol::results::OpaqueLazyAssociatedTestResults,
};
use async_trait::async_trait;
use parking_lot::Mutex;

use super::{ResultsPersistedCell, ResultsStream, SharedPersistResults, WithResultsStream};

#[derive(Default, Clone)]
pub struct ResultsWrapper(Arc<Mutex<Option<OpaqueLazyAssociatedTestResults>>>);

impl ResultsWrapper {
    pub fn get(self) -> OpaqueResult<OpaqueLazyAssociatedTestResults> {
        let mut guard = self.0.lock();
        guard
            .take()
            .ok_or_else(|| "Results already taken".located(here!()))
    }
}

pub struct ResultsLoader {
    pub results: ResultsWrapper,
}

#[async_trait]
impl WithResultsStream for ResultsLoader {
    async fn with_results_stream<'a>(
        self: Box<Self>,
        results_stream: ResultsStream<'a>,
    ) -> super::Result<()> {
        let ResultsStream { mut stream, len: _ } = results_stream;

        let loaded = OpaqueLazyAssociatedTestResults::read_results_lines(&mut stream)
            .await
            .located(here!())?;

        *self.results.0.lock() = Some(loaded);

        Ok(())
    }
}

pub async fn retrieve_results(
    cell: &ResultsPersistedCell,
    persistence: &SharedPersistResults,
) -> OpaqueResult<OpaqueLazyAssociatedTestResults> {
    let results = ResultsWrapper::default();
    let results_loader = ResultsLoader {
        results: results.clone(),
    };

    cell.retrieve_with_callback(persistence, Box::new(results_loader))
        .await
        .located(here!())?;

    results.get()
}
