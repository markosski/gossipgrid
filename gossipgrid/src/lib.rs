use std::time::{SystemTime, UNIX_EPOCH};

pub mod cli;
pub mod env;
pub mod event;
mod gossip;
pub mod item;
pub mod node;
pub mod partition;
pub mod store;
pub mod sync;
pub mod web;

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

pub fn now_seconds() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as u32
}
