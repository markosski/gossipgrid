
pub mod transport;

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use crate::{gossip::HLC, partition::PartitionId};

pub struct SyncMessage {

}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncState {
    /// HLC represent last seen item HLC
    partitions: HashMap<PartitionId, HLC>
}

#[async_trait::async_trait]
pub trait Store: Send + Sync {
    async fn get_sync_state() -> Result<Option<SyncState>, SyncStoreError>;
    async fn write_sync_state(partition_id: PartitionId, item_hlc: HLC);
    async fn clear_state() -> Result<(), SyncStoreError>;
}

#[derive(Error, Debug)]                                                   
pub enum SyncStoreError {
    #[error("Error retrieving sync state: {0}")]                   
    SyncStoreOperationError(String),
}
