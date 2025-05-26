use crate::now_millis;
use crate::gossip::HLC;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type ItemId = String;

#[derive(Debug, Clone, Encode, Decode)]
pub struct Item {
    pub id: ItemId,
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
    pub id: Option<ItemId>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ItemSubmitResponse {
    pub success: Option<HashMap<String, String>>,
    pub error: Option<String>,
}
