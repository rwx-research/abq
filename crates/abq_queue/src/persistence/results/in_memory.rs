use std::{collections::HashMap, sync::Arc};

use abq_utils::{
    error::{ErrorLocation, ResultLocation},
    here,
    net_protocol::{results::ResultsLine, workers::RunId},
};
use async_trait::async_trait;
use serde_json::value::RawValue;
use tokio::sync::RwLock;

use super::{PersistResults, Result, ResultsStream, SharedPersistResults, WithResultsStream};

#[derive(Default, Clone)]
pub struct InMemoryPersistor {
    results: Arc<RwLock<HashMap<RunId, Vec<Box<RawValue>>>>>,
}

impl InMemoryPersistor {
    pub fn new_shared() -> SharedPersistResults {
        SharedPersistResults(Box::new(Self::default()))
    }
}

#[async_trait]
impl PersistResults for InMemoryPersistor {
    async fn dump(&self, run_id: &RunId, results: ResultsLine) -> Result<()> {
        let raw_line = serde_json::value::to_raw_value(&results).located(here!())?;
        let mut results = self.results.write().await;
        let entry = results.entry(run_id.clone()).or_default();
        entry.push(raw_line);
        Ok(())
    }

    async fn dump_to_remote(&self, _run_id: &RunId) -> Result<()> {
        Ok(())
    }

    async fn with_results_stream(
        &self,
        run_id: &RunId,
        callback: Box<dyn WithResultsStream + Send>,
    ) -> Result<()> {
        let results = self.results.read().await;
        let json_lines = results
            .get(run_id)
            .ok_or_else(|| "results not found for run ID".located(here!()))?;

        let mut readable_json_lines_buffer: Vec<u8> = Vec::new();
        for json_line in json_lines {
            serde_json::to_writer(&mut readable_json_lines_buffer, json_line).located(here!())?;
            readable_json_lines_buffer.push(b'\n');
        }

        let len = readable_json_lines_buffer.len();
        let mut slice = readable_json_lines_buffer.as_slice();

        callback
            .with_results_stream(ResultsStream {
                stream: Box::new(&mut slice),
                len,
            })
            .await
    }

    fn boxed_clone(&self) -> Box<dyn PersistResults> {
        Box::new(self.clone())
    }
}
