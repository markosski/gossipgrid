pub mod memory_store;

use std::collections::HashMap;

use thiserror::Error;

use crate::{item::ItemEntry, partition::{PartitionId}};

#[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// Get item by key and range_key
    async fn get(&self, partition: &PartitionId, key: &str, range_key: &str) -> Result<Option<ItemEntry>, DataStoreError>;

    /// Get many items by key and optional range_key
    /// If range_key is None, get all items with the given key up to the limit
    async fn get_many(&self, partition: &PartitionId, key: &str, range_key: Option<&str>, limit: usize) -> Result<Vec<ItemEntry>, DataStoreError>;

    /// Insert item by key and range_key
    /// If an item with the same key and range_key exists, it should be updated
    async fn insert(&mut self, partition: &PartitionId, key: String, range_key: &str, value: ItemEntry) -> Result<(), DataStoreError>;

    /// Remove item by key and optional range_key
    /// If range_key is None, remove all items with the given key
    async fn remove(&mut self, partition: &PartitionId, key: Option<&str>, range_key: &str) -> Result<(), DataStoreError>;

    /// Get all items in the delta
    async fn get_all_delta(&self) -> Result<Vec<ItemEntry>, DataStoreError>;

    /// Clear all items in the delta
    async fn clear_all_delta(&mut self) -> Result<(), DataStoreError>;

    /// Remove item from delta by key
    async fn remove_from_delta(&mut self, key: &str) -> Result<(), DataStoreError>;

    /// Get count of items in the delta
    async fn delta_count(&self) -> Result<usize, DataStoreError>;

    /// Get counts of items per partition
    /// Counts should exclude deleted items
    async fn partition_counts(&self) -> Result<HashMap<PartitionId, usize>, DataStoreError>;
}



#[derive(Error, Debug)]                                                   
pub enum DataStoreError {
    #[error("Store `Get Item` operation error: {0}")]                   
    StoreGetOperationError(String),
    #[error("Store `Insert Item` operation error: {0}")]                   
    StoreInsertOperationError(String),
    #[error("Store `Remove Item` operation error: {0}")]                   
    StoreRemoveOperationError(String),
    #[error("Store general operation error: {0}")]                   
    StoreOperationError(String),
}

