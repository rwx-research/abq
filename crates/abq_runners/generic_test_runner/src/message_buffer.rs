//! Utilities for buffering and acting on messages (from the queue), refilling messages on-demand.

#![allow(unused)] // TODO remove

use std::future::IntoFuture;
use std::io;

use tokio::sync::mpsc;
use tokio::sync::oneshot;

enum Msg<T> {
    BatchSize(usize),
    Msg(T),
}

impl<T> std::fmt::Debug for Msg<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BatchSize(arg0) => f.debug_tuple("BatchSize").field(arg0).finish(),
            Self::Msg(arg0) => f.debug_tuple("Msg").finish(),
        }
    }
}

pub(crate) struct BatchedProducer<T> {
    msg_tx: mpsc::Sender<Msg<T>>,
    refill_rx: mpsc::Receiver<()>,
    exit_rx: oneshot::Receiver<()>,
}

pub(crate) struct BatchedConsumer<T> {
    msg_rx: mpsc::Receiver<Msg<T>>,
    refill_tx: mpsc::Sender<()>,
    exit_tx: oneshot::Sender<()>,
    refill_strategy: RefillStrategy,
}

/// When a producer should refill the batch buffer of messages.
pub(crate) enum RefillStrategy {
    /// The producer should begin to refill the buffer with a new batch when the last batch is
    /// half-consumed.
    HalfConsumed,
}

/// Create (producer, consumer) channels for processing batched messages.
fn channel<T>(
    capacity: usize,
    refill_strategy: RefillStrategy,
) -> (BatchedProducer<T>, BatchedConsumer<T>) {
    // Double the asked capacity so we have space to fit new messages when we receive an ask to
    // refill the channel.
    let (msg_tx, msg_rx) = mpsc::channel(capacity * 2);
    let (refill_tx, refill_rx) = mpsc::channel(1);
    let (exit_tx, exit_rx) = oneshot::channel();

    let producer = BatchedProducer {
        msg_tx,
        refill_rx,
        exit_rx,
    };
    let consumer = BatchedConsumer {
        msg_rx,
        refill_tx,
        exit_tx,
        refill_strategy,
    };

    (producer, consumer)
}

/// Creates producer and consumer channels for processing batched messages, returning a future that
/// resolves when both are complete.
///
/// The future completes when
/// - the corresponding [MsgProducer] closes the channel
/// - `process_msg` returns an error, which will stop the producer, and return all pending
///   messages in the channel.
pub(crate) async fn run_channels<T, E, P, PFut, I, Iter, C, CFut>(
    capacity: usize,
    refill_strategy: RefillStrategy,
    fetch_msgs: P,
    process_msg: C,
) -> Result<(), (E, Vec<T>)>
where
    P: Fn() -> PFut,
    PFut: IntoFuture<Output = (I, Completed)>,
    I: IntoIterator<IntoIter = Iter>,
    Iter: ExactSizeIterator<Item = T>,

    C: Fn(T) -> CFut,
    CFut: IntoFuture<Output = Result<(), E>>,
{
    let (tx, rx) = channel(capacity, refill_strategy);
    let tx = tx.start(fetch_msgs);
    let rx = rx.start(process_msg);
    let ((), rx_result) = tokio::join!(tx, rx);
    rx_result
}

pub(crate) struct Completed(bool);

impl<T> BatchedProducer<T> {
    /// Returns a future that runs the producer, with a task to fetch messages.
    /// The future completes only when the consumer has indicated it has completed as well.
    pub async fn start<F, Fut, I, Iter>(mut self, fetch_msgs: F)
    where
        F: Fn() -> Fut,
        Fut: IntoFuture<Output = (I, Completed)>,
        I: IntoIterator<IntoIter = Iter>,
        Iter: ExactSizeIterator<Item = T>,
    {
        let mut msgs;
        let mut completed;
        loop {
            (msgs, completed) = fetch_msgs().await;

            let msgs = msgs.into_iter();
            self.msg_tx
                .send(Msg::BatchSize(msgs.len()))
                .await
                .expect("internal error - message channel closed early by consumer");
            for msg in msgs {
                self.msg_tx
                    .send(Msg::Msg(msg))
                    .await
                    .expect("internal error - message channel closed early by consumer");
            }

            if completed.0 {
                return;
            }

            tokio::select! {
                _ = self.refill_rx.recv() => {
                    continue;
                }
                _ = &mut self.exit_rx => {
                    return;
                }
            }
        }
    }
}

impl<T> BatchedConsumer<T> {
    /// Returns a future that runs the consumer, with a task to process messages.
    /// The future completes only when
    /// - the corresponding [MsgProducer] closes the channel
    /// - `process_msg` returns an error, which will stop the producer, and return all pending
    ///   messages in the channel.
    pub async fn start<F, Fut, E>(mut self, process_msg: F) -> Result<(), (E, Vec<T>)>
    where
        F: Fn(T) -> Fut,
        Fut: IntoFuture<Output = Result<(), E>>,
    {
        let mut refill_at = 0;
        let mut processed = 0;
        loop {
            let msg = match self.msg_rx.recv().await {
                Some(msg) => msg,
                None => return Ok(()), // all done
            };

            let msg = match msg {
                Msg::BatchSize(n) => {
                    match self.refill_strategy {
                        RefillStrategy::HalfConsumed => {
                            refill_at = n / 2;
                        }
                    }
                    continue;
                }
                Msg::Msg(msg) => msg,
            };

            processed += 1;

            if processed >= refill_at {
                processed = 0;
                let refill_tx = self.refill_tx.clone();
                tokio::spawn(async move { refill_tx.send(()).await });
            }

            if let Err(err) = process_msg(msg).await {
                self.exit_tx
                    .send(())
                    .expect("internal error - producer channel closed unexpectedly");

                let mut remaining = vec![];
                while let Some(msg) = self.msg_rx.recv().await {
                    match msg {
                        Msg::BatchSize(_) => continue,
                        Msg::Msg(msg) => remaining.push(msg),
                    }
                }
                return Err((err, remaining));
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        convert::Infallible,
        sync::{atomic::AtomicU8, Arc},
    };

    use abq_utils::atomic;

    use super::{channel, run_channels, Completed, RefillStrategy};

    #[tokio::test]
    async fn refill_with_reload_one() {
        let fetch_count = AtomicU8::new(0);
        let fetch_msgs = || async {
            fetch_count.fetch_add(1, atomic::ORDERING);
            let loaded = fetch_count.load(atomic::ORDERING);
            ([loaded], Completed(loaded == 5))
        };

        let next_expected = Arc::new(AtomicU8::new(1));
        let process_msg = |n| {
            let next_expected = next_expected.clone();
            async move {
                assert_eq!(next_expected.load(atomic::ORDERING), n);
                next_expected.fetch_add(1, atomic::ORDERING);
                Result::<_, Infallible>::Ok(())
            }
        };

        let result = run_channels(2, RefillStrategy::HalfConsumed, fetch_msgs, process_msg).await;
        assert!(result.is_ok(), "{:?}", result);

        assert_eq!(fetch_count.load(atomic::ORDERING), 5);
    }

    #[tokio::test]
    async fn refill_with_reload_three() {
        let fetch_count = AtomicU8::new(0);
        let fetch_msgs = || async {
            let n = fetch_count.fetch_add(1, atomic::ORDERING);
            match n {
                0 => (vec![1, 2, 3], Completed(false)),
                1 => (vec![4, 5, 6], Completed(false)),
                2 => (vec![7, 8], Completed(true)),
                _ => unreachable!(),
            }
        };

        let next_expected = Arc::new(AtomicU8::new(1));
        let process_msg = |n| {
            let next_expected = next_expected.clone();
            async move {
                assert_eq!(next_expected.load(atomic::ORDERING), n);
                next_expected.fetch_add(1, atomic::ORDERING);
                Result::<_, Infallible>::Ok(())
            }
        };

        let result = run_channels(6, RefillStrategy::HalfConsumed, fetch_msgs, process_msg).await;
        assert!(result.is_ok(), "{:?}", result);

        assert_eq!(fetch_count.load(atomic::ORDERING), 3);
    }

    #[tokio::test]
    async fn err_return_pending_msgs() {
        let fetch_count = AtomicU8::new(0);
        let fetch_msgs = || async {
            let n = fetch_count.fetch_add(1, atomic::ORDERING);
            match n {
                0 => (vec![1, 2, 3, 4], Completed(false)),
                1 => (vec![5, 6, 7, 8], Completed(false)),
                _ => unreachable!(),
            }
        };

        let process_msg = |n| async move { Err(n) };

        let result = run_channels(4, RefillStrategy::HalfConsumed, fetch_msgs, process_msg).await;
        assert!(result.is_err(), "{:?}", result);

        let (n, rem_msgs) = result.unwrap_err();
        assert_eq!(n, 1);
        assert_eq!(rem_msgs, &[2, 3, 4]);

        assert_eq!(fetch_count.load(atomic::ORDERING), 1);
    }
}
