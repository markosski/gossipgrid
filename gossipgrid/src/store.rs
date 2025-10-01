pub mod memory_store;

use std::{collections::HashMap, str::FromStr};

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{item::{Item, ItemEntry}, partition::PartitionId};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode, Serialize, Deserialize)]
pub struct PartitionKey(pub String);
impl PartitionKey {
    pub fn value(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode, Serialize, Deserialize)]
pub struct RangeKey(pub String);
impl RangeKey {
    pub fn value(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode, Serialize, Deserialize)]
pub struct StorageKey {
    pub partition_key: PartitionKey,
    pub range_key: Option<RangeKey>,
}

impl StorageKey {
    pub fn new(partition_key: PartitionKey, range_key: Option<RangeKey>) -> Self {
        StorageKey {
            partition_key,
            range_key,
        }
    }

    pub fn to_string(&self) -> String {
        match &self.range_key {
            Some(rk) => format!("{}_{}", self.partition_key.value(), rk.value()),
            None => self.partition_key.value().to_string(),
        }
    }
}

impl FromStr for StorageKey {
    type Err = DataStoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {

        let parts: Vec<&str> = s.splitn(2, '_').collect();
        let partition_key = PartitionKey(parts[0].to_string());
        let range_key = if parts.len() > 1 {
            Some(RangeKey(parts[1].to_string()))
        } else {
            None
        };
        Ok(StorageKey::new(partition_key, range_key))
    }
}

#[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// Get item by key and range_key
    async fn get(&self, partition: &PartitionId, key: &StorageKey) -> Result<Option<ItemEntry>, DataStoreError>;

    /// Get many items by key and optional range_key
    /// If range_key is None, get all items with the given key up to the limit
    async fn get_many(&self, partition: &PartitionId, key: &StorageKey, limit: usize) -> Result<Vec<ItemEntry>, DataStoreError>;

    /// Insert item by key and range_key
    /// If an item with the same key and range_key exists, it should be updated
    async fn insert(&mut self, partition: &PartitionId, key: &StorageKey, value: Item) -> Result<(), DataStoreError>;

    /// Remove item by key and optional range_key
    /// If range_key is None, remove all items with the given key
    async fn remove(&mut self, partition: &PartitionId, key: &StorageKey) -> Result<(), DataStoreError>;

    /// Get all items in the delta
    async fn get_all_delta(&self) -> Result<Vec<ItemEntry>, DataStoreError>;

    /// Clear all items in the delta
    async fn clear_all_delta(&mut self) -> Result<(), DataStoreError>;

    /// Remove item from delta by key
    async fn remove_from_delta(&mut self, key: &StorageKey) -> Result<(), DataStoreError>;

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
    #[error("Storage key parsing error: {0}")]                   
    StorageKeyParsingError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_storage_key_with_pk_and_rk() {
        let parsed_storage_key = "partition1_range1".parse::<StorageKey>().unwrap();
        let expected_storage_key = StorageKey::new(
            PartitionKey("partition1".to_string()),
            Some(RangeKey("range1".to_string())),
        );

        assert_eq!(parsed_storage_key, expected_storage_key);
    }

    #[test]
    fn test_parse_storage_key_with_pk_and_no_rk() {
        let parsed_storage_key = "partition1".parse::<StorageKey>().unwrap();
        let expected_storage_key = StorageKey::new(
            PartitionKey("partition1".to_string()),
            None,
        );

        assert_eq!(parsed_storage_key, expected_storage_key);
    }
}