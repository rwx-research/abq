//! Utilities for managing shutdown of ABQ servers.

use std::{
    io,
    sync::{atomic::AtomicBool, Arc},
};

use tokio::sync::broadcast;

use crate::atomic::ORDERING;

pub struct ShutdownManager {
    retired: Arc<AtomicBool>,
    immediate_shutdown_tx: broadcast::Sender<()>,
    immediate_shutdown_rx: Option<broadcast::Receiver<()>>,
    shutdown: bool,
}

impl Default for ShutdownManager {
    fn default() -> Self {
        let (immediate_shutdown_tx, immediate_shutdown_rx) = broadcast::channel(1);

        Self {
            retired: Arc::new(AtomicBool::new(false)),
            immediate_shutdown_tx,
            immediate_shutdown_rx: Some(immediate_shutdown_rx),
            shutdown: false,
        }
    }
}

impl ShutdownManager {
    pub fn new_pair() -> (Self, ShutdownReceiver) {
        let (immediate_shutdown_tx, immediate_shutdown_rx) = broadcast::channel(1);

        let manager = Self {
            retired: Arc::new(AtomicBool::new(false)),
            immediate_shutdown_tx,
            immediate_shutdown_rx: None,
            shutdown: false,
        };
        let recv = ShutdownReceiver {
            retired: Arc::clone(&manager.retired),
            immediate_shutdown_rx,
        };

        (manager, recv)
    }

    pub fn add_receiver(&mut self) -> ShutdownReceiver {
        ShutdownReceiver {
            retired: self.retired.clone(),
            immediate_shutdown_rx: self
                .immediate_shutdown_rx
                .take()
                .unwrap_or_else(|| self.immediate_shutdown_tx.subscribe()),
        }
    }

    pub fn is_retired(&self) -> bool {
        self.retired.load(ORDERING)
    }

    pub fn retire(&mut self) {
        self.retired.store(true, ORDERING)
    }

    pub fn shutdown_immediately(&mut self) -> io::Result<usize> {
        assert!(!self.shutdown);
        self.shutdown = true;
        self.immediate_shutdown_tx.send(()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "could not send shutdown message",
            )
        })
    }
}

pub struct ShutdownReceiver {
    retired: Arc<AtomicBool>,
    immediate_shutdown_rx: broadcast::Receiver<()>,
}

impl ShutdownReceiver {
    pub async fn recv_shutdown_immediately(&mut self) -> io::Result<()> {
        self.immediate_shutdown_rx
            .recv()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e.to_string()))
    }

    pub fn get_retirement_cell(&self) -> RetirementCell {
        RetirementCell {
            retired: self.retired.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RetirementCell {
    retired: Arc<AtomicBool>,
}

impl RetirementCell {
    pub fn is_retired(&self) -> bool {
        self.retired.load(ORDERING)
    }

    /// Notify the shutdown managers and other receivers that we were asked to retire.
    pub fn notify_asked_to_retire(&self) {
        self.retired.store(true, ORDERING)
    }
}
