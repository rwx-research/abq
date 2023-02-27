use std::sync::{atomic::AtomicUsize, Arc};

use abq_utils::atomic;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

/// Counts live workers in a pool.
pub struct LiveCount {
    count: Arc<AtomicUsize>,
    completed_signal_rx_handle: Option<JoinHandle<()>>,
    rx_all_completed: Option<oneshot::Receiver<()>>,
}

impl LiveCount {
    pub async fn new(count: usize) -> (Self, CompletedSignaler) {
        let (tx_completed, mut rx_completed) = mpsc::channel(count);
        let (tx_all_completed, rx_all_completed) = oneshot::channel();
        let count = Arc::new(AtomicUsize::new(count));

        let completed_signals_handler = {
            let count = count.clone();
            tokio::spawn(async move {
                loop {
                    rx_completed
                        .recv()
                        .await
                        .expect("all live count signals died without sending completion");
                    let prev_val = count.fetch_sub(1, atomic::ORDERING);
                    if prev_val == 1 {
                        // all done
                        break;
                    }
                }

                // NB: the all-completed receiver can be dropped without being awaited; as such, if
                // there is an error here, swallow it.
                let _ = tx_all_completed.send(());
            })
        };

        let me = Self {
            count,
            completed_signal_rx_handle: Some(completed_signals_handler),
            rx_all_completed: Some(rx_all_completed),
        };
        let signaler = CompletedSignaler { tx_completed };

        (me, signaler)
    }

    /// Returns a future that resolves when all workers have completed.
    /// Cancel-safe.
    pub async fn wait(&mut self) {
        if let Some(rx_all_completed) = &mut self.rx_all_completed {
            rx_all_completed
                .await
                .expect("all_completed sender dropped before handler joined");
            self.rx_all_completed = None;
        }
        if let Some(handle) = &mut self.completed_signal_rx_handle {
            handle
                .await
                .expect("completed handler dropped before live count dropped");
            self.completed_signal_rx_handle = None;
        }
    }

    /// Read the number of live workers at this instant.
    pub fn read(&self) -> usize {
        self.count.load(atomic::ORDERING)
    }
}

/// A signal to [LiveCount] that a worker has completed.
#[derive(Clone)]
pub struct CompletedSignaler {
    tx_completed: mpsc::Sender<()>,
}

impl CompletedSignaler {
    pub fn completed(self) {
        let _ = self.tx_completed.blocking_send(());
    }
}

#[cfg(test)]
mod test {
    use std::{thread, time::Duration};

    use abq_run_n_times::n_times;

    use super::LiveCount;

    #[tokio::test]
    #[n_times(1_000)]
    async fn smoke() {
        let count = 10;

        let (mut live_count, signal) = LiveCount::new(count).await;

        assert_eq!(live_count.read(), count);

        let handle = thread::spawn(move || {
            for _ in 1..count {
                signal.clone().completed();
            }
            signal.completed();
        });

        live_count.wait().await;
        assert_eq!(live_count.read(), 0);
        handle.join().unwrap();
    }

    #[tokio::test]
    #[n_times(1_000)]
    async fn does_not_die_until_all_workers_die() {
        let count = 10;

        let (mut live_count, signal) = LiveCount::new(count).await;

        assert_eq!(live_count.read(), count);

        let handle = thread::spawn({
            let signal = signal.clone();
            move || {
                for _ in 1..count {
                    signal.clone().completed();
                }
            }
        });

        handle.join().unwrap();
        tokio::select! {
            _ = live_count.wait() => panic!("{}", live_count.read()),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }

        let _ = signal;
    }

    #[tokio::test]
    #[n_times(1_000)]
    async fn fuzz_cancel_safety() {
        let count = 10;

        let (mut live_count, signal) = LiveCount::new(count).await;

        assert_eq!(live_count.read(), count);

        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            for _ in 1..count {
                signal.clone().completed();
            }
            signal.completed();
        });

        handle.join().unwrap();
        loop {
            tokio::select! {
                _ = live_count.wait() => break,
                _ = tokio::time::sleep(Duration::from_micros(10)) => {}
            }
        }
    }
}
