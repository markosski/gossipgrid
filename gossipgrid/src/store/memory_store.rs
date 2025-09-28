use std::collections::HashMap;

use log::info;

use crate::partition::{PartitionId};

// Add this import or definition for ItemId
use crate::item::{self, Item, ItemEntry};
use crate::store::{DataStoreError, StorageKey};

use super::Store; // or define: type ItemId = String;

pub struct InMemoryStore {
    item_partitions: HashMap<PartitionId, HashMap<StorageKey, Item>>,
    items_delta: HashMap<StorageKey, Item>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        InMemoryStore {
            item_partitions: HashMap::new(),
            items_delta: HashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl Store for InMemoryStore {
    async fn get(&self, partition: &PartitionId, key: &StorageKey) -> Result<Option<ItemEntry>, DataStoreError> {
        let maybe_partition = self.item_partitions.get(&partition);
        if let Some(partition) = maybe_partition {
            Ok(partition.get(key).map(|x| ItemEntry::new(key.clone(), x.clone())))
        } else {
            Ok(None)
        }
    }

    async fn get_many(&self, partition: &PartitionId, key: &StorageKey, limit: usize) -> Result<Vec<ItemEntry>, DataStoreError> {
        // TODO: loop through items match on parittion key and partial match on range key
        // return up to limit items
        Ok(vec![])
    }

    async fn insert(&mut self, partition: &PartitionId, key: &StorageKey, value: Item) -> Result<(), DataStoreError> {
        let maybe_partition = self.item_partitions.get_mut(&partition);
        info!(
            "Adding item to partition: {:?}, key: {}, value: {:?}",
            partition, key.to_string(), value
        );

        if let Some(partition) = maybe_partition {
            partition.insert(key.clone(), value.clone());
            self.items_delta.insert(key.clone(), value);
        } else {
            let mut new_partition = HashMap::new();
            new_partition.insert(key.clone(), value.clone());
            self.item_partitions.insert(*partition, new_partition);
            self.items_delta.insert(key.clone(), value);
        }
        Ok(())
    }

    async fn remove(&mut self, partition: &PartitionId, key: &StorageKey) -> Result<(), DataStoreError> {
        info!("Removing item from partition: {:?}, key: {}", partition, key.to_string());

        let maybe_partition = self.item_partitions.get_mut(&partition);
        if let Some(partition) = maybe_partition {
            partition.remove(key);
        }
        Ok(())
    }

    async fn get_all_delta(&self) -> Result<Vec<ItemEntry>, DataStoreError> {
        Ok(self.items_delta.iter().map(|(k, v)| ItemEntry::new(k.clone(), v.clone())).collect())
    }

    async fn clear_all_delta(&mut self) -> Result<(), DataStoreError> {
        self.items_delta.clear();
        Ok(())
    }

    async fn remove_from_delta(&mut self, key: &StorageKey) -> Result<(), DataStoreError> {
        self.items_delta.remove(key);
        Ok(())
    }

    async fn partition_counts(&self) -> Result<HashMap<PartitionId, usize>, DataStoreError> {
        let mut counts = HashMap::new();
        for (partition, items) in &self.item_partitions {
            let mut counter = 0;
            for item in items.values() {
                match item.status {
                    item::ItemStatus::Tombstone(_) => continue,
                    item::ItemStatus::Active => {
                        counter += 1;
                    }
                }
            }
            counts.insert(*partition, counter);
        }
        Ok(counts)
    }

    async fn delta_count(&self) -> Result<usize, DataStoreError> {
        Ok(self.items_delta.values().len())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        gossip::HLC, item::{Item, ItemStatus}, partition::PartitionMap, store::{PartitionKey, RangeKey}
    };

    #[tokio::test]
    async fn test_insert_item_pk_only() {
        use super::*;
        let mut store = InMemoryStore::new();
        let partition_map = PartitionMap::new(&10, &2);

        let storage_key = StorageKey { partition_key: PartitionKey("item1".to_string()), range_key: None };
        let partition = partition_map.hash_key(&storage_key.to_string());

        let _ = store
            .insert(
                &partition,
                &storage_key,
                Item {
                    message: "test".as_bytes().to_vec(),
                    status: ItemStatus::Active,
                    hlc: HLC::new(),
                },
            )
            .await;

        let retrieved_item = store.get(&partition, &storage_key).await.unwrap();
        assert!(retrieved_item.is_some());
        let item_entry = retrieved_item.unwrap();
        assert_eq!(item_entry.storage_key, storage_key);
        assert_eq!(item_entry.item.message, "test".as_bytes().to_vec());
    }

    #[tokio::test]
    async fn test_insert_item_pk_and_rk() {
        use super::*;
        let mut store = InMemoryStore::new();
        let partition_map = PartitionMap::new(&10, &2);

        let storage_key = StorageKey { partition_key: PartitionKey("123".to_string()), range_key: Some(RangeKey("456".to_string())) };
        let partition = partition_map.hash_key(&storage_key.partition_key.value());

        let _ = store
            .insert(
                &partition,
                &storage_key,
                Item {
                    message: "test".as_bytes().to_vec(),
                    status: ItemStatus::Active,
                    hlc: HLC::new(),
                },
            )
            .await;

        let retrieved_item = store.get(&partition, &storage_key).await.unwrap();
        assert!(retrieved_item.is_some());
        let item_entry = retrieved_item.unwrap();
        assert_eq!(item_entry.storage_key, storage_key);
        assert_eq!(item_entry.item.message, "test".as_bytes().to_vec());
    }

    #[tokio::test]
    async fn test_partition_counts() {
        use super::*;
        let mut store = InMemoryStore::new();

        let partition1 = PartitionId(1);
        let partition2 = PartitionId(2);

        let storage_key1 = StorageKey { partition_key: PartitionKey("item1".to_string()), range_key: None };
        let storage_key2 = StorageKey { partition_key: PartitionKey("item2".to_string()), range_key: None };
        let storage_key3 = StorageKey { partition_key: PartitionKey("item3".to_string()), range_key: None };

        let _ = store
            .insert(
                &partition1,
                &storage_key1,
                Item {
                    message: "test1".as_bytes().to_vec(),
                    status: ItemStatus::Active,
                    hlc: HLC::new(),
                },
            )
            .await;

        let _ = store
            .insert(
                &partition1,
                &storage_key2,
                Item {
                        message: "test2".as_bytes().to_vec(),
                        status: ItemStatus::Active,
                        hlc: HLC::new(),
                },
            )
            .await;

        let _ = store
            .insert(
                &partition2,
                &storage_key3,
                Item {
                    message: "test3".as_bytes().to_vec(),
                    status: ItemStatus::Active,
                    hlc: HLC::new(),
                },
            )
            .await;

        let counts = store.partition_counts().await.unwrap();
        assert_eq!(counts.get(&partition1), Some(&2));
        assert_eq!(counts.get(&partition2), Some(&1));
    }
}
