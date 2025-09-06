use crate::env::Env;
use crate::gossip::{receive_gossip, send_gossip_on_interval, HLC};
use crate::item::{ItemEntry, ItemId, ItemStatus};
use crate::partition::{PartitionMap};
use crate::store::{Store};
use crate::{now_millis, web};
use bincode::{Decode, Encode};
use log::{error, info};
use names::Generator;
use tokio::try_join;
use ttl_cache::TtlCache;

use std::cmp::min;
use std::collections::{hash_map, BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

const DELTA_STATE_EXPIRY: Duration = Duration::from_secs(60);

/// Main entry point for the node
pub async fn start_node(web_addr: String, local_addr: String, node_state: Arc<RwLock<NodeState>>, env: Arc<Env>) -> anyhow::Result<()> {
    let sync_flag = Arc::new(Mutex::new(false));
    let node_address = {
        let node_state = node_state.read().await;
        node_state.get_address().clone()
    };

    info!("node={}; Starting application: {}", &node_address, &local_addr);

    // Initialize socket
    let socket = Arc::new(UdpSocket::bind(&local_addr).await.unwrap());

    // Fire and forget gossip tasks
    let sending = tokio::spawn(send_gossip_on_interval(
        local_addr.clone(),
        socket.clone(),
        node_state.clone(),
        sync_flag.clone(),
        env.clone()
    ));
    let receiving = tokio::spawn(receive_gossip(
        socket.clone(),
        node_state.clone(),
        sync_flag.clone(),
        env.clone()
    ));

    // Initiate HTTP server
    info!("node={}; Starting Web Server: {}", &node_address, &web_addr);
    let web_server = tokio::spawn(web::web_server(web_addr.clone(), socket.clone(), node_state.clone(), env.clone()));

    // Panic if one of the tasks fails
    try_join!(sending, receiving, web_server)?;

    Ok(())
}

pub type NodeId = String;
pub type Peers = HashMap<NodeId, Node>;

#[derive(Debug)]
pub enum NodeState {
    PreJoin(PreJoinNode),
    JoinedSyncing(JoinedNode),
    Joined(JoinedNode),
    Disconnected(DisconnectedNode),
}

#[derive(Debug)]
pub struct DisconnectedNode {
    pub address: NodeId,
    pub web_port: u16,
    pub node_hlc: HLC,
}

#[derive(Debug)]
pub struct PreJoinNode {
    pub address: NodeId,
    pub web_port: u16,
    pub peer_node: String,
    pub node_hlc: HLC,
}

pub struct JoinedNode {
    pub address: NodeId,
    pub web_port: u16,
    pub all_peers: Peers,
    pub cluster_config: ClusterConfig,
    pub partition_map: PartitionMap,
    pub node_hlc: HLC,
    // Stores the items that need to be gossiped to other nodes
    pub items_delta_state: HashMap<ItemId, DeltaAckState>,
    // Stores the origin of the delta item to avoid sending it back to the same node
    pub items_delta_cache: TtlCache<ItemId, DeltaOrigin>,
    pub index: BTreeMap<HLC, HashSet<String>>,
    pub next_node_index: u8,
}

/// Manual Debug implementation for JoinedNode, skipping the store field
impl std::fmt::Debug for JoinedNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinedNode")
            .field("all_peers", &self.all_peers)
            .field("cluster_config", &self.cluster_config)
            .field("partition_map", &self.partition_map)
            .field("this_node", &self.address)
            .field("this_node_web_port", &self.web_port)
            .field("node_hlc", &self.node_hlc)
            .field("items_delta_state", &self.items_delta_state)
            .field("index", &self.index)
            .field("next_node_index", &self.next_node_index)
            .finish()
    }
}

#[derive(Debug, Clone, Encode, Decode, PartialEq)]
pub struct Node {
    pub address: String,
    pub web_port: u16,
    pub last_seen: u64,
}

#[derive(Debug, Clone, Encode, Decode, PartialEq)]
pub struct ClusterConfig {
    pub cluster_size: u8,
    pub partition_count: u16,
    pub replication_factor: u8,
}

/// Store acknowledgments for each item and the nodes it wasw sent to
#[derive(Debug, Clone)]
pub struct DeltaAckState {
    pub peers_pending: HashSet<String>,
    pub created_at: u64,
}

/// Store the origin node when delta was shared so that we can temporarily cache it
#[derive(Debug, Clone)]
pub struct DeltaOrigin {
    pub node_id: NodeId,
    pub item_hlc: HLC,
}

impl NodeState {
    pub fn init(
        local_addr: String,
        local_web_port: u16,
        seed_peer: Option<String>,
        cluster_config: Option<ClusterConfig>,
    ) -> NodeState {
        let mut generator = Generator::default();
        let mut known_peers = HashMap::new();
        known_peers.insert(
            local_addr.clone(),
            Node {
                address: local_addr.clone(),
                web_port: local_web_port,
                last_seen: 0,
            },
        );

        if let Some(peer) = &seed_peer {
            known_peers.insert(
                peer.clone(),
                Node {
                    address: peer.clone(),
                    web_port: 0,
                    last_seen: now_millis(),
                },
            );
        }

        if let Some(cluster_config) = &cluster_config {
            let mut node = JoinedNode {
                all_peers: known_peers.clone(),
                cluster_config: cluster_config.clone(),
                partition_map: PartitionMap::new(
                    &cluster_config.partition_count,
                    &cluster_config.replication_factor,
                ),
                address: local_addr,
                web_port: local_web_port,
                node_hlc: HLC {
                    timestamp: 0, // Initialize HLC with zero timestamp otherwise new node will be seen as source of truth for cluster state
                    counter: 0,
                },
                // store: Box::new(InMemoryStore::new()),
                index: BTreeMap::new(),
                items_delta_state: HashMap::new(),
                items_delta_cache: TtlCache::new(10000),
                next_node_index: 0,
            };
            node.partition_map.assign(&node.all_peers.keys().cloned().collect::<Vec<_>>());
            NodeState::Joined(node)
        } else if let Some(peer_address) = seed_peer {
            NodeState::PreJoin(PreJoinNode {
                address: local_addr,
                web_port: local_web_port,
                peer_node: peer_address.clone(),
                node_hlc: HLC {
                    timestamp: 0, // Initialize HLC with zero timestamp otherwise new node will be seen as source of truth for cluster state
                    counter: 0,
                },
            })
        } else {
            panic!("NodeState must be initialized with either a cluster config or a seed peer address");
        }
    }

    pub fn get_address(&self) -> &String {
        match self {
            NodeState::Joined(joined_node) => &joined_node.address,
            NodeState::JoinedSyncing(joined_node) => &joined_node.address,
            NodeState::PreJoin(pre_join_node) => &pre_join_node.address,
            NodeState::Disconnected(disconnected_node) => &disconnected_node.address
        }
    }
}

impl DisconnectedNode {
    pub fn get_address(&self) -> &String {
        &self.address
    }
}

impl PreJoinNode {
    pub fn get_address(&self) -> &String {
        &self.address
    }

    pub fn get_peer_node(&self) -> &String {
        &self.peer_node
    }

    pub fn to_joined_state(&mut self, cluster_config: ClusterConfig, partition_map: PartitionMap) -> JoinedNode {
        JoinedNode {
            address: self.address.clone(),
            web_port: self.web_port,
            all_peers: HashMap::new(),
            cluster_config,
            partition_map,
            node_hlc: self.node_hlc.clone(),
            // store: Box::new(InMemoryStore::new()),
            index: BTreeMap::new(),
            items_delta_state: HashMap::new(),
            items_delta_cache: TtlCache::new(10000),
            next_node_index: 0,
        }
    }
}


impl JoinedNode {
    pub fn get_address(&self) -> &String {
        &self.address
    }

    // TODO: consder moving this to gossip module
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
            .unwrap_or_else(|| self.get_address().clone());

        self.next_node_index = self.next_node_index.wrapping_add(1);
        info!("node={}; Selected next node to gossip: {:?}", self.get_address(), &selected_peer);
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

    pub fn cluster_size(&self) -> u8 {
        self.all_peers.len() as u8
    }

    pub fn get_node(&self, node_id: &String) -> Option<&Node> {
        self.all_peers.get(node_id)
    }

    pub fn all_peers(&self) -> hash_map::Iter<'_, String, Node> {
        self.all_peers.iter()
    }

    pub fn other_peers(&self) -> HashMap<String, Node> {
        self.all_peers
            .iter()
            .filter(|(k, _)| *k != &self.address)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn add_node(&mut self, node: Node) {
        self.all_peers.insert(node.address.clone(), node);
        self.node_hlc.tick_hlc(now_millis());
    }

    pub fn remove_node(&mut self, node_id: &String) {
        if self.all_peers.remove(node_id).is_some() {
            self.node_hlc.tick_hlc(now_millis());
        } else {
            error!("node={}; Node {} not found in all peers", self.get_address(), node_id);
        }
    }


    async fn add_item(&mut self, entry: ItemEntry, from_node: &str, store: &mut RwLockWriteGuard<'_, Box<dyn Store>>) -> Option<ItemEntry> {
        let vnode = self.partition_map.hash_key(&entry.item.id);
        if let Some(existing_entry) = store.get(&vnode, &entry.item.id).await {
            if entry.hlc > existing_entry.hlc {
                let new_entry = ItemEntry {
                    item: entry.item.clone(),
                    status: entry.status.clone(),
                    hlc: HLC::merge(&existing_entry.hlc, &entry.hlc, now_millis()),
                };

                store.add(&vnode, entry.item.id.clone(), new_entry.clone()).await;
                info!("node={}; Updated item {} with new entry: {:?}", self.get_address(), entry.item.id, &new_entry);

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
                info!("node={}; Received an update for item {} that is older or equal to the existing entry, ignoring it. Incoming {:?} vs Existing {:?}", 
                    self.get_address(), entry.item.id, &entry.hlc, &existing_entry.hlc);
                None
            }
        } else {
            // Add new item
            let new_entry = entry.clone();
            store.add(&vnode, entry.item.id.clone(), new_entry.clone()).await;
            info!("node={}; Added new item {}: {:?}", self.get_address(), entry.item.id, &new_entry);

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

    pub async fn add_items(&mut self, items: &[ItemEntry], from_node: &str, store: RwLockWriteGuard<'_, Box<dyn Store>>) -> Vec<ItemEntry> {
        let mut added_items = vec![];
        let mut store_ref = store;
        for item in items {
            if let Some(new_entry) = self.add_item(item.clone(), from_node, &mut store_ref).await {
                added_items.push(new_entry.clone());
            }
        }
        added_items
    }

    /// Purge delta state for acknowledged delta items
    ///
    /// The reason why we check each item insead of a batch of items as a whole is that we want to ensure 
    /// that we only remove the items that are acknowledged by the peer and not remove items that are still pending acknowledgment, 
    /// e.g. in case new itmes are added to the delta state.
    pub async fn reconcile_delta_state(&mut self, from_node: String, ack_delta_items: &[ItemEntry], store: &mut RwLockWriteGuard<'_, Box<dyn Store>>) {
        for ack_item in ack_delta_items {
            if let Some(delta_ack) = self.items_delta_state.get_mut(&ack_item.item.id) {
                delta_ack.peers_pending.remove(&from_node);

                if delta_ack.peers_pending.is_empty() {
                    info!("node={}; removing item {} from delta state as all peers have acknowledged it", self.get_address(), &ack_item.item.id);
                    store.remove_delta_item(&ack_item.item.id).await;
                    self.items_delta_state.remove(&ack_item.item.id);
                }
            }
        }
    }

    /// Add new items to delta state
    pub fn add_delta_state(&mut self, items: &[ItemEntry], delta_ack_state: DeltaAckState) {
        for item in items {
            if let Some(delta_ack) = self.items_delta_state.get_mut(&item.item.id) {
                delta_ack.peers_pending.extend(delta_ack_state.peers_pending.clone());
                delta_ack.created_at = now_millis();
            } else {
                self.items_delta_state.insert(item.item.id.clone(), delta_ack_state.clone());
            }
            info!("node={}; Added to delta state for item {}: {:?}", self.get_address(), &item.item.id, &delta_ack_state);
        }
    }

    /// Expire state if we received new state for the item
    pub fn invalidate_delta_state(&mut self, item_id: &ItemId) {
        self.items_delta_state.remove(item_id);
        info!("node={}; Invalidated delta state for item {}", self.get_address(), &item_id);
    }

    /// Remove items from delta state after some time
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

    pub async fn clear_delta(&mut self, store: &mut RwLockWriteGuard<'_, Box<dyn Store>>) {
        store.clear_all_delta().await;
    }

    pub fn get_delta_state(&self) -> HashMap<ItemId, DeltaAckState> {
        self.items_delta_state.clone()
    }

    /// for each item check if item is in the cache for the node and if it is 
    pub async fn get_delta_for_node(&self, node: &NodeId, store: &RwLockReadGuard<'_, Box<dyn Store>>) -> Vec<ItemEntry> {
        let mut items = vec![];
        for item_entry in store.get_all_delta().await.iter() {
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

    pub async fn remove_item(&mut self, item_id: &String, from_node: &str, store: &mut RwLockWriteGuard<'_, Box<dyn Store>>) -> bool {
        let vnode = self.partition_map.hash_key(item_id);

        if let Some(existing_entry) = store.get(&vnode, item_id).await {
            let mut new_item_entry = existing_entry.clone();
            let now_millis = now_millis();
            new_item_entry.status = ItemStatus::Tombstone(now_millis);
            new_item_entry.hlc.tick_hlc(now_millis);

            store.add(&vnode, item_id.clone(), new_item_entry.clone()).await;

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

    /// Updates it's peers with peers from upstream
    /// 
    /// # Arguments
    /// 
    /// * `joining_cluster` - whether or not we need to update partition map after new node joined the cluster
    /// 
    pub fn take_peers(&mut self, peers: &Peers, joining_cluster: bool) {
        for (addr, node) in peers.iter() {
            self.all_peers
                .entry(addr.clone())
                .and_modify(|existing| {
                    // Keep the most recent last_seen
                    if node.last_seen > existing.last_seen {
                        *existing = node.clone();
                    }
                })
                .or_insert_with(|| node.clone());
        }

        if joining_cluster {
            // If we are joining the cluster, we need to update the partition map
            self.partition_map.assign(&self.all_peers.keys().cloned().collect::<Vec<_>>());
            self.node_hlc.tick_hlc(now_millis());
        }
    }

    pub async fn get_item(&self, item_id: &ItemId, store: &RwLockReadGuard<'_, Box<dyn Store>>) -> Option<ItemEntry> {
        let vnode = self.partition_map.hash_key(item_id);
        store.get(&vnode, item_id).await
    }

    pub async fn items_count(&self, store: &RwLockReadGuard<'_, Box<dyn Store>>) -> usize {
        store.count().await
    }

    pub async fn items_since(&self, hlc: &HLC, store: &RwLockReadGuard<'_, Box<dyn Store>>) -> Vec<ItemEntry> {
        let mut items = vec![];
        for (_, set) in self.index.range(hlc..).rev() {
            for item_id in set {
            let vnode = self.partition_map.hash_key(item_id);
        
                if let Some(item) = store.get(&vnode, item_id).await {
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

        let mut node_state = NodeState::init("127.0.0.1:1000".to_string(), 3001, None, Some(cluster_config));
        
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


        // match &mut node_state {
        //     NodeState::Joined(joined_node) => {
        //         joined_node.add_items(vec!(item1.clone(), item2.clone()), "nodeA").await;
        //         joined_node.add_delta_state(&[item1.clone(), item2.clone()], DeltaAckState {
        //             peers_pending: ["nodeA".to_string()].iter().cloned().collect(),
        //             created_at: now_millis(),
        //         });
        //         let vnode = joined_node.partition_map.hash_key(&item1.item.id);
        //         let item1_timestamp = joined_node.store.get(&vnode, &item1.item.id).await.unwrap().hlc.timestamp;
                
        //         joined_node.reconcile_delta_state("nodeA".to_string(), &[ItemEntry {
        //             item: Item {
        //                 id: "task1".to_string(),
        //                 message: "task1 message".to_string(),
        //                 submitted_at: 100,
        //             },
        //             status: ItemStatus::Active,
        //             hlc: HLC { timestamp: item1_timestamp, counter: 0 },
        //         }]).await;

        //         assert_eq!(joined_node.store.delta_count().await, 1);
        //     }
        //     _ => panic!("Node is not in Joined state"),
        // }
    }
}
