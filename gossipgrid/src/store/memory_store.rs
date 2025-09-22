use std::collections::HashMap;

use log::info;

use crate::partition::{PartitionId};

// Add this import or definition for ItemId
use crate::item::{self, ItemEntry, ItemId};
use crate::store::DataStoreError;

use super::Store; // or define: type ItemId = String;

pub struct InMemoryStore {
    item_partitions: HashMap<PartitionId, HashMap<ItemId, ItemEntry>>,
    items_delta: HashMap<ItemId, ItemEntry>,
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
    async fn get(&self, partition: &PartitionId, key: &str) -> Result<Option<&ItemEntry>, DataStoreError> {
        let maybe_partition = self.item_partitions.get(&partition);
        if let Some(partition) = maybe_partition {
            Ok(partition.get(key))
        } else {
            Ok(None)
        }
    }

    async fn get_many(&self, partition: &PartitionId, key: &str, limit: usize) -> Result<Vec<&ItemEntry>, DataStoreError> {
        Ok(vec![])
    }

    async fn insert(&mut self, partition: &PartitionId, key: String, value: ItemEntry) -> Result<(), DataStoreError> {
        let maybe_partition = self.item_partitions.get_mut(&partition);
        info!(
            "Adding item to partition: {:?}, key: {}, value: {:?}",
            partition, key, value
        );

        if let Some(partition) = maybe_partition {
            partition.insert(key.clone(), value.clone());
            self.items_delta.insert(key, value);
        } else {
            let mut new_partition = HashMap::new();
            new_partition.insert(key.clone(), value.clone());
            self.item_partitions.insert(*partition, new_partition);
            self.items_delta.insert(key, value);
        }
        Ok(())
    }

    async fn remove(&mut self, partition: &PartitionId, key: &str) -> Result<(), DataStoreError> {
        info!("Removing item from partition: {:?}, key: {}", partition, key);

        let maybe_partition = self.item_partitions.get_mut(&partition);
        if let Some(partition) = maybe_partition {
            partition.remove(key);
        }
        Ok(())
    }

    async fn get_all_delta(&self) -> Result<Vec<ItemEntry>, DataStoreError> {
        Ok(self.items_delta.values().cloned().collect())
    }

    async fn clear_all_delta(&mut self) -> Result<(), DataStoreError> {
        self.items_delta.clear();
        Ok(())
    }

    async fn remove_from_delta(&mut self, key: &str) -> Result<(), DataStoreError> {
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
        gossip::HLC,
        item::{Item, ItemStatus},
    };

    #[tokio::test]
    async fn test_partition_counts() {
        use super::*;
        let mut store = InMemoryStore::new();

        let partition1 = PartitionId(1);
        let partition2 = PartitionId(2);

        let _ = store
            .insert(
                &partition1,
                "item1".to_string(),
                ItemEntry {
                    item: Item {
                        id: "item1".to_string(),
                        message: "test1".to_string(),
                        submitted_at: 0,
                    },
                    status: ItemStatus::Active,
                    hlc: HLC::new(),
                },
            )
            .await;

        let _ = store
            .insert(
                &partition1,
                "item2".to_string(),
                ItemEntry {
                    item: Item {
                        id: "item2".to_string(),
                        message: "test2".to_string(),
                        submitted_at: 0,
                    },
                    status: ItemStatus::Active,
                    hlc: HLC::new(),
                },
            )
            .await;

        let _ = store
            .insert(
                &partition2,
                "item3".to_string(),
                ItemEntry {
                    item: Item {
                        id: "item3".to_string(),
                        message: "test3".to_string(),
                        submitted_at: 0,
                    },
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
