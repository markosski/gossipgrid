use std::collections::HashMap;

use log::info;
use tokio::sync::RwLock;

use crate::partition::{PartitionId};

// Add this import or definition for ItemId
use crate::item::{self, Item, ItemEntry, ItemStatus};
use crate::store::{DataStoreError, StorageKey};

use super::Store; // or define: type ItemId = String;

pub struct InMemoryStore {
    item_partitions: RwLock<HashMap<PartitionId, HashMap<StorageKey, Item>>>,
    items_delta: RwLock<HashMap<StorageKey, Item>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        InMemoryStore {
            item_partitions: RwLock::new(HashMap::new()),
            items_delta: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl Store for InMemoryStore {
    async fn get(&self, partition_id: &PartitionId, key: &StorageKey) -> Result<Option<ItemEntry>, DataStoreError> {
        let item_partitions = self.item_partitions.read().await;
        let maybe_partition = item_partitions.get(&partition_id);
        if let Some(partition) = maybe_partition {
            let item_entry = partition.get(key).map(|x| ItemEntry::new(key.clone(), x.clone()));
            if let Some(item) = item_entry && item.item.status == ItemStatus::Active {
                if item.item.status == ItemStatus::Active {
                    Ok(Some(item))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    async fn get_many(&self, partition_id: &PartitionId, key: &StorageKey, limit: usize) -> Result<Vec<ItemEntry>, DataStoreError> {
        let item_partitions = self.item_partitions.read().await;
        let maybe_partition = item_partitions.get(&partition_id);
        let mut data: Vec<ItemEntry> = vec!();
        let mut counter = 0;
        if let Some(partition) = maybe_partition {
            for (storage_key, item) in partition.iter() {
                if counter == limit {
                    break;
                }

                if item.status == ItemStatus::Active && storage_key.partition_key == key.partition_key {
                    if let (Some(range_key), Some(key_range_key)) = (&storage_key.range_key, &key.range_key) 
                    && range_key.value().contains(key_range_key.value()) {
                        data.push(ItemEntry { storage_key: storage_key.clone(), item: item.clone() });
                        counter += 1;
                    } else if key.range_key.is_none() {
                        data.push(ItemEntry { storage_key: storage_key.clone(), item: item.clone() });
                        counter += 1;
                    }
                }
            }
        }
        Ok(data)
    }

    async fn insert(&self, partition: &PartitionId, key: &StorageKey, value: Item) -> Result<(), DataStoreError> {
        let mut item_partitions = self.item_partitions.write().await;
        let maybe_partition = item_partitions.get_mut(&partition);
        info!(
            "Adding item to partition: {:?}, key: {}, value: {:?}",
            partition, key.to_string(), value
        );

        let mut items_delta = self.items_delta.write().await;
        if let Some(partition) = maybe_partition {
            partition.insert(key.clone(), value.clone());
            items_delta.insert(key.clone(), value);
        } else {
            let mut new_partition = HashMap::new();
            new_partition.insert(key.clone(), value.clone());
            item_partitions.insert(*partition, new_partition);
            items_delta.insert(key.clone(), value);
        }
        Ok(())
    }

    async fn remove(&self, partition: &PartitionId, key: &StorageKey) -> Result<(), DataStoreError> {
        info!("Removing item from partition: {:?}, key: {}", partition, key.to_string());
        let mut item_partitions = self.item_partitions.write().await;

        let maybe_partition = item_partitions.get_mut(&partition);
        if let Some(partition) = maybe_partition {
            partition.remove(key);
        }
        Ok(())
    }

    async fn get_all_delta(&self) -> Result<Vec<ItemEntry>, DataStoreError> {
        let items_delta = self.items_delta.read().await;
        Ok(items_delta.iter().map(|(k, v)| ItemEntry::new(k.clone(), v.clone())).collect())
    }

    async fn clear_all_delta(&self) -> Result<(), DataStoreError> {
        let mut items_delta = self.items_delta.write().await;
        items_delta.clear();
        Ok(())
    }

    async fn remove_from_delta(&self, key: &StorageKey) -> Result<(), DataStoreError> {
        let mut items_delta = self.items_delta.write().await;
        items_delta.remove(key);
        Ok(())
    }

    async fn partition_counts(&self) -> Result<HashMap<PartitionId, usize>, DataStoreError> {
        let item_partitions = self.item_partitions.read().await;
        let mut counts = HashMap::new();
        for (partition, items) in item_partitions.iter() {
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
        let items_delta = self.items_delta.read().await;
        Ok(items_delta.values().len())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        gossip::HLC, partition::PartitionMap, store::{PartitionKey, RangeKey}
    };

    #[tokio::test]
    async fn test_insert_item_pk_only() {
        use super::*;
        let store = InMemoryStore::new();
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
        let store = InMemoryStore::new();
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
        let store = InMemoryStore::new();

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
