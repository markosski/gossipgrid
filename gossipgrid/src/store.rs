pub mod memory_store;

use std::collections::HashMap;

use crate::{item::ItemEntry, partition::VNode};

#[async_trait::async_trait]
pub trait Store: Send + Sync {
    async fn get(&self, vnode: &VNode, key: &str) -> Option<&ItemEntry>;

    async fn add(&mut self, vnode: &VNode, key: String, value: ItemEntry);

    async fn remove(&mut self, vnode: &VNode, key: &str);

    async fn get_all_delta(&self) -> Vec<ItemEntry>;

    async fn clear_all_delta(&mut self);

    async fn remove_delta_item(&mut self, key: &str);

    /// Get counts of items per partition (VNode)
    /// Counts should exclude deleted items
    async fn partition_counts(&self) -> HashMap<VNode, usize>;

    async fn delta_count(&self) -> usize;
}
