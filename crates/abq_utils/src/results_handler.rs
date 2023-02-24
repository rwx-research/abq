use parking_lot::Mutex;
use std::sync::Arc;

use crate::net_protocol::queue::AssociatedTestResults;
use async_trait::async_trait;

/// Send a batch of test results.
#[async_trait]
pub trait NotifyResults {
    async fn send_results(&mut self, results: Vec<AssociatedTestResults>);
}

pub type ResultsHandler = Box<dyn NotifyResults + Send>;

/// A results handler that can be shared by cloning.
pub trait SharedNotifyResults: NotifyResults {
    fn boxed_clone(&self) -> SharedResultsHandler;
}

pub type SharedResultsHandler = Box<dyn SharedNotifyResults + Send + Sync>;

//

pub type SharedAssociatedTestResults = Arc<Mutex<Vec<AssociatedTestResults>>>;

/// A results handler that collects all test results.
/// For use in the wrapped runner and tests.
#[derive(Clone)]
pub struct StaticResultsHandler {
    test_results: SharedAssociatedTestResults,
}

impl StaticResultsHandler {
    pub fn new(test_results: SharedAssociatedTestResults) -> Self {
        Self { test_results }
    }
}

#[async_trait]
impl NotifyResults for StaticResultsHandler {
    async fn send_results(&mut self, results: Vec<AssociatedTestResults>) {
        self.test_results.lock().extend(results)
    }
}

impl SharedNotifyResults for StaticResultsHandler {
    fn boxed_clone(&self) -> SharedResultsHandler {
        Box::new(self.clone())
    }
}

//

#[derive(Clone, Copy)]
pub struct NoopResultsHandler;

#[async_trait]
impl NotifyResults for NoopResultsHandler {
    async fn send_results(&mut self, _results: Vec<AssociatedTestResults>) {}
}

impl SharedNotifyResults for NoopResultsHandler {
    fn boxed_clone(&self) -> SharedResultsHandler {
        Box::new(*self)
    }
}
