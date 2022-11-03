use std::sync::atomic;

pub const ORDERING: atomic::Ordering = atomic::Ordering::SeqCst; // keep it simple
