use crate::gossip::{HLC, now_millis};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Encode, Decode)]
pub struct Item {
    pub id: String,
    pub message: String, // Example metadata
    pub submitted_at: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct ItemEntry {
    pub item: Item,
    pub status: ItemStatus,
    pub hlc: HLC,
}

impl ItemEntry {
    pub fn new(item: Item) -> ItemEntry {
        ItemEntry {
            item: item,
            status: ItemStatus::Active,
            hlc: HLC::new().tick_hlc(now_millis()),
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum ItemStatus {
    Active,
    Tombstone(u64),
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct ItemSubmit {
    pub message: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ItemSubmitResponse {
    pub success: Option<String>,
    pub error: Option<String>,
}
