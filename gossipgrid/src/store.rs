pub mod memory_store;

use std::collections::HashMap;

use thiserror::Error;

use crate::{item::ItemEntry, partition::{PartitionId}};

#[async_trait::async_trait]
pub trait Store: Send + Sync {
    async fn get(&self, partition: &PartitionId, key: &str) -> Result<Option<ItemEntry>, DataStoreError>;

    async fn get_many(&self, partition: &PartitionId, key: &str, limit: usize) -> Result<Vec<ItemEntry>, DataStoreError>;

    async fn insert(&mut self, partition: &PartitionId, key: String, value: ItemEntry) -> Result<(), DataStoreError>;

    async fn remove(&mut self, partition: &PartitionId, key: &str) -> Result<(), DataStoreError>;

    async fn get_all_delta(&self) -> Result<Vec<ItemEntry>, DataStoreError>;

    async fn clear_all_delta(&mut self) -> Result<(), DataStoreError>;

    async fn remove_from_delta(&mut self, key: &str) -> Result<(), DataStoreError>;

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

