//! Utilities for buffering and acting on messages (from the queue), refilling messages on-demand.

use std::future::IntoFuture;

use tokio::sync::mpsc;

enum Msg<T> {
    BatchSize(usize),
    Msg(T),
}

impl<T> std::fmt::Debug for Msg<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BatchSize(arg0) => f.debug_tuple("BatchSize").field(arg0).finish(),
            Self::Msg(_) => f.debug_tuple("Msg").finish(),
        }
    }
}

pub(crate) struct BatchedProducer<T> {
    msg_tx: mpsc::Sender<Msg<T>>,
    refill_rx: mpsc::Receiver<()>,
}

pub(crate) struct BatchedConsumer<T> {
    msg_rx: mpsc::Receiver<Msg<T>>,
    refill_tx: mpsc::Sender<()>,
    refill_strategy: RefillStrategy,

    /// Whether the consumer should attempt a refill if the refill strategy is met.
    /// This is true between the time a new batch is received and the refill strategy is met, and
    /// false for all times after the refill strategy is met but before the current batch is
    /// exhausted.
    eligible_for_refill: bool,

    current_batch_size: usize,
    current_batch_processed: usize,
}

/// When a producer should refill the batch buffer of messages.
pub(crate) enum RefillStrategy {
    /// The producer should begin to refill the buffer with a new batch when the last batch is
    /// half-consumed.
    HalfConsumed,
}

/// Create (producer, consumer) channels for processing batched messages.
pub(crate) fn channel<T>(
    capacity: usize,
    refill_strategy: RefillStrategy,
) -> (BatchedProducer<T>, BatchedConsumer<T>) {
    // Double the asked capacity so we have space to fit new messages when we receive an ask to
    // refill the channel.
    let (msg_tx, msg_rx) = mpsc::channel(capacity * 2);
    let (refill_tx, refill_rx) = mpsc::channel(1);

    let producer = BatchedProducer { msg_tx, refill_rx };
    let consumer = BatchedConsumer {
        msg_rx,
        refill_tx,
        refill_strategy,

        eligible_for_refill: true,

        current_batch_size: 0,
        current_batch_processed: 0,
    };

    (producer, consumer)
}

pub(crate) struct Completed(pub bool);

impl<T> BatchedProducer<T> {
    /// Returns a future that runs the producer, with a task to fetch messages.
    /// The future completes when the generator indicates that it is complete, or the consumer
    /// exits.
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

            // If we fail to send the message, the consumer has exited early and we have nothing
            // more to do.
            if self.msg_tx.send(Msg::BatchSize(msgs.len())).await.is_err() {
                return;
            };
            for msg in msgs {
                if self.msg_tx.send(Msg::Msg(msg)).await.is_err() {
                    return;
                }
            }

            if completed.0 {
                return;
            }

            if self.refill_rx.recv().await.is_none() {
                // Consumer has exited
                return;
            }
        }
    }
}

impl<T> BatchedConsumer<T> {
    /// Returns the next message in the channel, or [None] if the channel is complete.
    pub async fn recv(&mut self) -> Option<T> {
        // If the channel is closed, we're all done.
        let msg = self.msg_rx.recv().await?;

        let msg = match msg {
            Msg::BatchSize(size) => {
                self.eligible_for_refill = true;
                self.current_batch_size = size;
                self.current_batch_processed = 0;

                let msg = self.msg_rx.recv().await?;

                match msg {
                    Msg::BatchSize(_) => panic!("Unexpected batch size message"),
                    Msg::Msg(msg) => msg,
                }
            }
            Msg::Msg(msg) => msg,
        };

        self.current_batch_processed += 1;

        let should_refill = self.eligible_for_refill
            && match self.refill_strategy {
                RefillStrategy::HalfConsumed => {
                    self.current_batch_processed >= self.current_batch_size / 2
                }
            };

        if should_refill {
            // At this point the refill channel should always be empty, because we have not yet
            // requested a refill and the channel has a capacity of 1 message. As such, fire
            // off the request in-path, as this call is likely to be very cheap and will wake
            // the refill task without the additional cost(s) of a background task.
            //
            // If the message fails to be sent, the producer has already reached the end of the
            // producing stream and has exited, leaving us nothing more to do.
            let _err = self.refill_tx.send(()).await;

            self.eligible_for_refill = false;
        }

        Some(msg)
    }

    /// Shuts down the consumer and producer, returning all pending messages from the producer. The
    /// producer will not asked to refill the message channel.
    pub async fn flush(self) -> Vec<T> {
        let Self {
            mut msg_rx,
            refill_tx,

            refill_strategy: _,
            eligible_for_refill: _,
            current_batch_size: _,
            current_batch_processed: _,
        } = self;

        // Close the refill channel, so that the producer knows that we are not
        // waiting any more and can exit.
        drop(refill_tx);

        let mut remaining = vec![];
        while let Some(msg) = msg_rx.recv().await {
            match msg {
                Msg::BatchSize(_) => continue,
                Msg::Msg(msg) => remaining.push(msg),
            }
        }
        remaining
    }
}

#[cfg(test)]
mod test {
    use std::{
        convert::Infallible,
        future::IntoFuture,
        sync::{atomic::AtomicU8, Arc},
    };

    use abq_utils::atomic;

    use super::{channel, Completed, RefillStrategy};

    async fn run_channels<T, E, P, PFut, I, Iter, C, CFut>(
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
        let (tx, mut rx) = channel(capacity, refill_strategy);
        let tx = tx.start(fetch_msgs);

        let run_rx_to_completion = async {
            while let Some(msg) = rx.recv().await {
                if let Err(e) = process_msg(msg).await {
                    let remaining = rx.flush().await;
                    return Err((e, remaining));
                }
            }

            Ok(())
        };

        let ((), rx_result) = tokio::join!(tx, run_rx_to_completion);
        rx_result
    }

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

    #[tokio::test]
    async fn run_channels_with_consumer_early_exit() {
        let fetch_count = AtomicU8::new(0);
        let fetch_msgs = || async {
            let n = fetch_count.fetch_add(1, atomic::ORDERING);
            match n {
                0 => (vec![1], Completed(false)),
                1 => (vec![2], Completed(false)),
                _ => unreachable!(),
            }
        };

        let (tx, mut rx) = channel(2, RefillStrategy::HalfConsumed);
        let tx = tx.start(fetch_msgs);

        let run_rx = async {
            // Pull one message and immediately exit - this should force a refill on the producer
            // side, but the consumer may not be around.
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg, 1);

            drop(rx);
        };

        let ((), ()) = tokio::join!(tx, run_rx);

        // Initial fetch + fetch after first pull
        assert_eq!(fetch_count.load(atomic::ORDERING), 2);
    }
}
