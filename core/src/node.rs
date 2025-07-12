use crate::gossip::{receive_gossip, send_gossip, HLC};
use crate::item::{ItemEntry, ItemId, ItemStatus};
use crate::partition::{PartitionMap, VNode};
use crate::store::memory_store::InMemoryStore;
use crate::store::Store;
use crate::{now_millis, web};
use bincode::{Decode, Encode};
use log::info;
use names::Generator;
use ttl_cache::TtlCache;

use std::cmp::min;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

const DELTA_STATE_EXPIRY: Duration = Duration::from_secs(60);

// Main entry point for the node
pub async fn start_node(web_addr: String, local_addr: String, node_memory: Arc<Mutex<NodeMemory>>) {
    // Generate name for this node
    let mut generator = Generator::default();
    let node_name = generator.next().unwrap();
    let sync_flag = Arc::new(Mutex::new(true));

    info!(name=node_name.as_str(); "node={}; Starting application: {}", &node_name, &local_addr);

    // Initialize socket
    let socket = Arc::new(UdpSocket::bind(&local_addr).await.unwrap());

    // Fire and forget gossip tasks
    let _ = tokio::spawn(send_gossip(
        node_name.clone(),
        local_addr.clone(),
        socket.clone(),
        node_memory.clone(),
        sync_flag.clone(),
    ));
    let _ = tokio::spawn(receive_gossip(
        node_name.clone(),
        socket.clone(),
        node_memory.clone(),
        sync_flag.clone(),
    ));

    // Initiate HTTP server
    info!(name=node_name.as_str(); "node={}; Starting Web Server: {}", &node_name, &web_addr);
    web::web_server(web_addr.clone(), socket.clone(), node_memory.clone()).await;
}

pub type NodeId = String;
pub type Peers = HashMap<NodeId, Node>;

pub struct NodeMemory {
    pub this_node: NodeId,
    pub this_node_web_port: u16,
    pub cluster_config: ClusterConfig,
    pub partition_map: PartitionMap,
    pub all_peers: Peers,
    pub node_hlc: HLC,
    store: Box<dyn Store>,
    pub items_delta_state: HashMap<ItemId, DeltaAckState>,
    pub items_delta_cache: TtlCache<ItemId, DeltaOrigin>,
    pub index: BTreeMap<HLC, HashSet<String>>,
    pub next_node_index: u8,
}

#[derive(Debug, Clone, Encode, Decode, PartialEq)]
pub struct Node {
    pub address: String,
    pub web_port: u16,
    pub last_seen: u64,
}

#[derive(Debug, Clone, Encode, Decode, PartialEq)]
pub struct ClusterConfig {
    pub cluster_size: u16,
    pub partition_count: u16,
    pub replication_factor: u8,
}

// Store acknowledgments for each item and the nodes it wasw sent to
#[derive(Debug, Clone)]
pub struct DeltaAckState {
    pub peers_pending: HashSet<String>,
    pub created_at: u64,
}

// Store the origin node when delta was shared so that we can temporarily cache it
#[derive(Debug, Clone)]
pub struct DeltaOrigin {
    pub node_id: NodeId,
    pub item_hlc: HLC,
}

impl NodeMemory {
    pub fn init(
        local_addr: String,
        local_web_port: u16,
        seed_peer: Option<String>,
        cluster_config: ClusterConfig,
    ) -> NodeMemory {
        let mut known_peers = HashMap::new();
        known_peers.insert(
            local_addr.clone(),
            Node {
                address: local_addr.clone(),
                web_port: local_web_port,
                last_seen: 0,
            },
        );

        if let Some(peer) = seed_peer {
            known_peers.insert(
                peer.clone(),
                Node {
                    address: peer.clone(),
                    web_port: 0,
                    last_seen: now_millis(),
                },
            );
        }

        let mut node = NodeMemory {
            this_node: local_addr,
            this_node_web_port: local_web_port,
            cluster_config: cluster_config.clone(),
            partition_map: PartitionMap::new(
                &cluster_config.partition_count,
                &cluster_config.replication_factor,
            ),
            all_peers: known_peers,
            node_hlc: HLC {
                timestamp: 0, // Initialize HLC with zero timestamp otherwise new node will be seen as source of truth for cluster state
                counter: 0,
            },
            store: Box::new(InMemoryStore::new()),
            index: BTreeMap::new(),
            items_delta_state: HashMap::new(),
            items_delta_cache: TtlCache::new(10000),
            next_node_index: 0,
        };

        node.partition_map.assign(&node.all_peers.keys().cloned().collect::<Vec<_>>());
        node
    }

    pub fn take_peers(&mut self, peers: Peers) {
        let peer_with_port_zero = peers.iter().filter(|x| x.1.web_port == 0).last().map(|(peer_id, _)| peer_id.clone());

        let port_with_port_zero = peer_with_port_zero.clone()
            .and_then(|peer_id| self.all_peers.get(&peer_id).map(|p| p.web_port));

        self.all_peers = peers;
        if let (Some(node_id), Some(port)) = (peer_with_port_zero, port_with_port_zero) {
            if let Some(node) = self.all_peers.get_mut(&node_id) {
                node.web_port = port;
            }
        }
    }

    pub async fn get_item(&self, item_id: &ItemId) -> Option<ItemEntry> {
        let vnode = self.partition_map.hash_key(item_id);
        self.store.get(&vnode, item_id)
            .await
    }

    pub fn gossip_count(&self) -> u8 {
        min(2, self.cluster_config.replication_factor)
    }

    pub fn next_node(&mut self) -> String {
        let other_peers = self.other_peers();
        let mut nodes: Vec<String> = other_peers.keys().cloned().collect();
        nodes.sort();

        let selected_peer = nodes
            .get(self.next_node_index as usize % other_peers.len())
            .cloned()
            .unwrap_or_else(|| self.this_node.clone());

        self.next_node_index = self.next_node_index.wrapping_add(1);
        selected_peer
    }
    
    pub fn next_nodes(&mut self, count: u8) -> Vec<String> {
        let peer_size = self.other_peers().len();
        let max_count = min(count as usize, peer_size);
        let mut selected_next = vec!();
        for _ in 0..max_count {
            selected_next.push(self.next_node());
        }
        selected_next
    }

    pub fn is_cluster_formed(&self) -> bool {
        self.cluster_size() == self.cluster_config.cluster_size
    }

    pub fn cluster_size(&self) -> u16 {
        self.all_peers.len() as u16
    }

    pub fn get_node(&self, node_id: &String) -> Option<Node> {
        self.all_peers.get(node_id).cloned()
    }

    pub fn all_peers(&self) -> HashMap<String, Node> {
        self.all_peers.clone()
    }

    pub fn other_peers(&self) -> HashMap<String, Node> {
        let mut other_peers = self.all_peers.clone();
        other_peers.remove(&self.this_node);
        other_peers
    }

    pub fn add_node(&mut self, node: Node) {
        self.all_peers.insert(node.address.clone(), node);
        self.node_hlc.tick_hlc(now_millis());
    }

    pub fn remove_node(&mut self, node_id: &String) {
        if self.all_peers.get(node_id).is_some() {
            self.all_peers.remove(node_id);
            self.node_hlc.tick_hlc(now_millis());
        }
    }

    pub async fn add_item(&mut self, entry: ItemEntry, from_node: &str) -> Option<ItemEntry> {
        let vnode = self.partition_map.hash_key(&entry.item.id);
        if let Some(existing_entry) = self.store.get(&vnode, &entry.item.id).await {
            if entry.hlc > existing_entry.hlc {
                let new_entry = ItemEntry {
                    item: entry.item.clone(),
                    status: entry.status.clone(),
                    hlc: HLC::merge(&existing_entry.hlc, &entry.hlc, now_millis()),
                };

                self.store.add(&vnode, entry.item.id.clone(), new_entry.clone()).await;
                info!("Updated item {} with new entry: {:?}", entry.item.id, &new_entry);

                // Update the delta and cache
                self.items_delta_cache.insert(
                    entry.item.id.clone(),
                    DeltaOrigin {
                        node_id: from_node.to_string(),
                        item_hlc: new_entry.hlc.clone(),
                    },
                    Duration::from_secs(60)
                );
                self.invalidate_delta_state(&entry.item.id);

                Some(new_entry.clone())
            } else {
                // If the new entry is older we do nothing
                info!("Received an update for item {} that is older or equal to the existing entry, ignoring it. Incoming {:?} vs Existing {:?}", entry.item.id, &entry.hlc, &existing_entry.hlc);
                None
            }
        } else {
            // Add new item
            let new_entry = entry.clone();
            self.store.add(&vnode, entry.item.id.clone(), new_entry.clone()).await;
            info!("Added new item {}: {:?}", entry.item.id, &new_entry);

            // Add to delta cache
            self.items_delta_cache.insert(
                entry.item.id.clone(),
                DeltaOrigin {
                    node_id: from_node.to_string(),
                    item_hlc: new_entry.hlc.clone(),
                },
                Duration::from_secs(60)
            );

            Some(new_entry.clone())
        }
    }

    pub async fn add_items(&mut self, items: Vec<ItemEntry>, from_node: &str) -> Vec<ItemEntry> {
        let mut added_items = vec![];
        for item in items {
            if let Some(new_entry) = self.add_item(item.clone(), from_node).await {
                added_items.push(new_entry.clone());
            }
        }
        added_items
    }

    // The reason why we check each item insead of a batch of items as a whole is that we want to ensure that we only remove the items that are acknowledged by the peer
    // and not remove items that are still pending acknowledgment, e.g. in case new itmes are added to the delta state.
    pub async fn reconcile_delta_state(&mut self, from_node: String, ack_delta_items: &[ItemEntry]) {
        for ack_item in ack_delta_items {
            if let Some(delta_ack) = self.items_delta_state.get_mut(&ack_item.item.id) {
                delta_ack.peers_pending.remove(&from_node);

                if delta_ack.peers_pending.is_empty() {
                    self.store.remove_delta_item(&ack_item.item.id).await;
                    self.items_delta_state.remove(&ack_item.item.id);
                }
            }
        }
    }

    pub fn add_delta_state(&mut self, items: &[ItemEntry], delta_ack_state: DeltaAckState) {
        for item in items {
            if let Some(delta_ack) = self.items_delta_state.get_mut(&item.item.id) {
                delta_ack.peers_pending.extend(delta_ack_state.peers_pending.clone());
                delta_ack.created_at = now_millis();
            } else {
                self.items_delta_state.insert(item.item.id.clone(), delta_ack_state.clone());
            }
            info!("Added to delta state for item {}: {:?}", &item.item.id, &delta_ack_state);
        }
    }

    // Expire state if we received new state for the item
    pub fn invalidate_delta_state(&mut self, item_id: &ItemId) {
        self.items_delta_state.remove(item_id);
        info!("Invalidated delta state for item {}", &item_id);
    }

    pub fn cleanup_expired_delta_state(&mut self) {
        let now = now_millis();
        let peers: std::collections::HashSet<_> = self.all_peers.keys().cloned().collect();

        self.items_delta_state.retain(|_item_id, delta_ack| {
            let age = now.saturating_sub(delta_ack.created_at) as u128;
            if age > DELTA_STATE_EXPIRY.as_millis() && delta_ack.peers_pending.iter().all(|peer| !peers.contains(peer)) {
                false
            } else {
                true
            }
        });
    }

    pub async fn clear_delta(&mut self) {
        self.store.clear_all_delta().await;
    }

    pub fn get_delta_state(&self) -> HashMap<ItemId, DeltaAckState> {
        self.items_delta_state.clone()
    }

    // for each item
    // check if item is in the cache for the node and if it is 
    pub async fn get_delta_for_node(&self, node: &NodeId) -> Vec<ItemEntry> {
        let mut items = vec![];
        for item_entry in self.store.get_all_delta().await.iter() {
            if let Some(cached_item) = self.items_delta_cache.get(item_entry.item.id.as_str()) {
                if !(cached_item.node_id == *node && cached_item.item_hlc >= item_entry.hlc) {
                    items.push(item_entry.clone());
                }
            } else {
                items.push(item_entry.clone());
            }
        }
        items
    }

    pub async fn remove_item(&mut self, item_id: &String, from_node: &str) -> bool {
        let vnode = self.partition_map.hash_key(item_id);

        if let Some(existing_entry) = self.store.get(&vnode, item_id).await {
            let mut new_item_entry = existing_entry.clone();
            let now_millis = now_millis();
            new_item_entry.status = ItemStatus::Tombstone(now_millis);
            new_item_entry.hlc.tick_hlc(now_millis);

            self.store.add(&vnode, item_id.clone(), new_item_entry.clone()).await;

            // Update the delta and cache
            self.items_delta_cache.insert(
                item_id.clone(),
                DeltaOrigin {
                    node_id: from_node.to_string(),
                    item_hlc: new_item_entry.hlc.clone(),
                },
                Duration::from_secs(60)
            );
            self.invalidate_delta_state(&item_id);
            return true;
        } else {
            return false; // Node not found
        }
    }

    pub async fn items_count(&self) -> usize {
        self.store.count().await
    }

    pub async fn items_since(&self, hlc: &HLC) -> Vec<ItemEntry> {
        let mut items = vec![];
        for (_, set) in self.index.range(hlc..).rev() {
            for item_id in set {
            let vnode = self.partition_map.hash_key(item_id);
        
                if let Some(item) = self.store.get(&vnode, item_id).await {
                    items.push(item.clone());
                }
            }
        }
        items
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::HLC;
    use crate::item::{Item, ItemStatus};

    #[tokio::test]
    async fn test_reconcile_delta() {
        let cluster_config = ClusterConfig {
            cluster_size: 2,
            partition_count: 8,
            replication_factor: 2,
        };

        let mut memory = NodeMemory::init("127.0.0.1:1000".to_string(), 3001, None, cluster_config);
        
        let item1 = ItemEntry {
            item: Item {
                id: "task1".to_string(),
                message: "task1 message".to_string(),
                submitted_at: 100,
            },
            status: ItemStatus::Active,
            hlc: HLC { timestamp: 100, counter: 0 },
        };
        let item2 = ItemEntry {
            item: Item {
                id: "task2".to_string(),
                message: "task2 message".to_string(),
                submitted_at: 101,
            },
            status: ItemStatus::Active,
            hlc: HLC { timestamp: 101, counter: 0 },
        };

        memory.add_items(vec!(item1.clone(), item2.clone()), "nodeA").await;
        memory.add_delta_state(&[item1.clone(), item2.clone()], DeltaAckState {
            peers_pending: ["nodeA".to_string()].iter().cloned().collect(),
            created_at: now_millis(),
        });

        let vnode = memory.partition_map.hash_key(&item1.item.id);
        let item1_timestamp = memory.store.get(&vnode, &item1.item.id).await.unwrap().hlc.timestamp;
        
        memory.reconcile_delta_state("nodeA".to_string(), &[ItemEntry {
            item: Item {
                id: "task1".to_string(),
                message: "task1 message".to_string(),
                submitted_at: 100,
            },
            status: ItemStatus::Active,
            hlc: HLC { timestamp: item1_timestamp, counter: 0 },
        }]).await;

        assert_eq!(memory.store.delta_count().await, 1);

    }
}
