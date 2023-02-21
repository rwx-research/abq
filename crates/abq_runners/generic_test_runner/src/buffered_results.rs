use abq_utils::net_protocol::queue::AssociatedTestResults;

use crate::SendTestResults;

pub(crate) struct BufferedResults<'a> {
    buffer: Vec<AssociatedTestResults>,
    batch_size: usize,
    send_test_results: SendTestResults<'a>,
}

impl<'a> BufferedResults<'a> {
    pub fn new(batch_size: usize, send_test_results: SendTestResults<'a>) -> Self {
        Self {
            buffer: Vec::with_capacity(batch_size),
            batch_size,
            send_test_results,
        }
    }
}

impl BufferedResults<'_> {
    pub async fn push(&mut self, result: AssociatedTestResults) {
        self.buffer.push(result);
        if self.buffer.len() >= self.batch_size {
            self.send().await;
        }
    }

    pub async fn flush(&mut self) {
        if !self.buffer.is_empty() {
            self.send().await;
        }
    }

    async fn send(&mut self) {
        let fresh_buf = Vec::with_capacity(self.buffer.len());
        let results = std::mem::replace(&mut self.buffer, fresh_buf);
        (self.send_test_results)(results).await
    }
}
