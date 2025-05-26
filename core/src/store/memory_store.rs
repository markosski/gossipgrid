use std::collections::HashMap;

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
    async fn get(&self, vnode: &VNode, key: &str) -> Option<ItemEntry> {
        let vnode_partition = self.item_partitions.get(vnode);
        if let Some(partition) = vnode_partition {
            partition.get(key).cloned()
        } else {
            None
        }
    }

    async fn add(&mut self, vnode: &VNode, key: String, value: ItemEntry) {
        let vnode_partition = self.item_partitions.get_mut(vnode);
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
        let vnode_partition = self.item_partitions.get_mut(vnode);
        if let Some(partition) = vnode_partition {
            partition.remove(key);
        }
    }

    async fn get_all_delta(&self) -> Vec<ItemEntry> {
        self.items_delta
            .values()
            .cloned()
            .collect()
    }

    async fn clear_all_delta(&mut self) {
        self.items_delta.clear();
    }

    async fn remove_delta_item(&mut self, key: &str) {
        self.items_delta.remove(key);
    }

    async fn count(&self) -> usize {
        let mut count = 0;
        for partition in self.item_partitions.values() {
            count += partition.len();
        }
        count
    }

    async fn delta_count(&self) -> usize {
        return self.items_delta.values().len();
    }
}