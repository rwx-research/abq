use std::{collections::HashMap, sync::Arc};

use abq_utils::{
    error::{ErrorLocation, ResultLocation},
    here,
    net_protocol::{
        results::{OpaqueLazyAssociatedTestResults, ResultsLine},
        workers::RunId,
    },
};
use async_trait::async_trait;
use serde_json::value::RawValue;
use tokio::sync::RwLock;

use super::{ArcResult, PersistResults, SharedPersistResults};

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
    async fn dump(&self, run_id: &RunId, results: ResultsLine) -> ArcResult<()> {
        let raw_line = serde_json::value::to_raw_value(&results).located(here!())?;
        let mut results = self.results.write().await;
        let entry = results.entry(run_id.clone()).or_default();
        entry.push(raw_line);
        Ok(())
    }

    async fn dump_to_remote(&self, _run_id: &RunId) -> ArcResult<()> {
        Ok(())
    }

    async fn get_results(&self, run_id: &RunId) -> ArcResult<OpaqueLazyAssociatedTestResults> {
        let results = self.results.read().await;
        let json_lines = results
            .get(run_id)
            .ok_or_else(|| "results not found for run ID".located(here!()))?;
        Ok(OpaqueLazyAssociatedTestResults::from_raw_json_lines(
            json_lines.clone(),
        ))
    }

    fn boxed_clone(&self) -> Box<dyn PersistResults> {
        Box::new(self.clone())
    }
}
