use std::collections::HashMap;

use log::info;

use crate::partition::VNode;

// Add this import or definition for ItemId
use crate::item::{ItemEntry, ItemId};

use super::Store; // or define: type ItemId = String;

pub struct InMemoryStore {
    item_partitions: HashMap<VNode, HashMap<ItemId, ItemEntry>>,
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
    async fn get(&self, vnode: &VNode, key: &str) -> Option<&ItemEntry> {
        let vnode_partition = self.item_partitions.get(vnode);
        if let Some(partition) = vnode_partition {
            partition.get(key)
        } else {
            None
        }
    }

    async fn add(&mut self, vnode: &VNode, key: String, value: ItemEntry) {
        let vnode_partition = self.item_partitions.get_mut(vnode);
        info!(
            "Adding item to vnode: {:?}, key: {}, value: {:?}",
            vnode, key, value
        );

        if let Some(partition) = vnode_partition {
            partition.insert(key.clone(), value.clone());
            self.items_delta.insert(key, value);
        } else {
            let mut new_partition = HashMap::new();
            new_partition.insert(key.clone(), value.clone());
            self.item_partitions.insert(*vnode, new_partition);
            self.items_delta.insert(key, value);
        }
    }

    async fn remove(&mut self, vnode: &VNode, key: &str) {
        info!("Removing item from vnode: {:?}, key: {}", vnode, key);

        let vnode_partition = self.item_partitions.get_mut(vnode);
        if let Some(partition) = vnode_partition {
            partition.remove(key);
        }
    }

    async fn get_all_delta(&self) -> Vec<ItemEntry> {
        self.items_delta.values().cloned().collect()
    }

    async fn clear_all_delta(&mut self) {
        self.items_delta.clear();
    }

    async fn remove_delta_item(&mut self, key: &str) {
        self.items_delta.remove(key);
    }

    async fn partition_counts(&self) -> HashMap<VNode, usize> {
        let mut counts = HashMap::new();
        for (vnode, items) in &self.item_partitions {
            counts.insert(*vnode, items.len());
        }
        counts
    }

    async fn delta_count(&self) -> usize {
        return self.items_delta.values().len();
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

        let vnode1 = 1;
        let vnode2 = 2;

        store
            .add(
                &vnode1,
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

        store
            .add(
                &vnode1,
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

        store
            .add(
                &vnode2,
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

        let counts = store.partition_counts().await;
        assert_eq!(counts.get(&vnode1), Some(&2));
        assert_eq!(counts.get(&vnode2), Some(&1));
    }
}
