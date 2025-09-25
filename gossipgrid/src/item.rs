use crate::gossip::HLC;
use crate::now_millis;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type ItemId = String;

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct Item {
    pub partitionKey: String,
    pub rangeKey: String,
    pub message: Vec<u8>, // Example metadata
    pub submitted_at: u64,
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub enum ItemStatus {
    Active,
    Tombstone(u64),
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct ItemSubmit {
    pub id: ItemId,
    pub message: String,
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct ItemUpdate {
    pub message: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ItemSubmitResponse {
    pub success: Option<HashMap<String, String>>,
    pub error: Option<String>,
}
