use std::time::{SystemTime, UNIX_EPOCH};

pub mod gossip;
pub mod item;
pub mod node;
pub mod partition;
pub mod web;
pub mod store;
pub mod cli;

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}