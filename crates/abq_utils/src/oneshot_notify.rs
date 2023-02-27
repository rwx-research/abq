//! Utilities for one-shot notification, particularly of shutdown messages.

use std::{future::Future, io, pin::Pin, sync::mpsc::TryRecvError, task};

use tokio::sync::oneshot::{self, Receiver, Sender};

pub struct OneshotTx(Sender<()>);

impl OneshotTx {
    pub fn notify(self) -> io::Result<()> {
        self.0.send(()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::ConnectionReset,
                "failed to send notification across oneshot channel",
            )
        })
    }
}

pub struct OneshotRx(Receiver<()>);

fn failed_recv() -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionReset,
        "failed to receive from oneshot channel",
    )
}

impl Future for OneshotRx {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map_err(|_| failed_recv())
    }
}

impl OneshotRx {
    pub fn try_recv(&mut self) -> Result<(), TryRecvError> {
        self.0.try_recv().map_err(|e| match e {
            oneshot::error::TryRecvError::Empty => TryRecvError::Empty,
            oneshot::error::TryRecvError::Closed => TryRecvError::Disconnected,
        })
    }
}

pub fn make_pair() -> (OneshotTx, OneshotRx) {
    let (tx, rx) = oneshot::channel();
    (OneshotTx(tx), OneshotRx(rx))
}

#[cfg(test)]
mod test {
    use super::make_pair;

    #[tokio::test]
    async fn send_recv() {
        let (tx, rx) = make_pair();
        let send_r = tx.notify();
        assert!(send_r.is_ok());
        let recv_r = rx.await;
        assert!(recv_r.is_ok());
    }
}
