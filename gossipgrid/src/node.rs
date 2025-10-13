use crate::env::Env;
use crate::gossip::{HLC, receive_gossip, send_gossip_on_interval};
use crate::item::{Item, ItemEntry, ItemStatus};
use crate::partition::{PartitionId, PartitionMap};
use crate::store::{StorageKey, Store};
use crate::{now_millis, web};
use bincode::{Decode, Encode};
use log::{error, info};
use thiserror::Error;
use tokio::try_join;
use ttl_cache::TtlCache;

use std::cmp::min;
use std::collections::{HashMap, HashSet, hash_map};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;

const DELTA_STATE_EXPIRY: Duration = Duration::from_secs(60);
const DELTA_ORIGIN_CACHE_INIT_CAPACITY: usize = 10000;
const DELTA_ORIGIN_CACHE_TTL_SECS: u64 = 60;

#[derive(Error, Debug)]
pub enum NodeError {
    #[error("Error starting node: {0}")]
    ErrorStartingNode(String),
    #[error("Error joining node: {0}")]
    ErrorJoiningNode(String),
    #[error("Error performing item operation: {0}")]
    ItemOperationError(String),
}

/// Main entry point for the node
//TODO: handle error
pub async fn start_node(
    web_addr: String,
    local_addr: String,
    node_state: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<(), NodeError> {
    let node_address = {
        let node_state = node_state.read().await;
        node_state.get_address().clone()
    };

    info!(
        "node={}; Starting application: {}",
        &node_address, &local_addr
    );

    // Initialize socket
    let socket = Arc::new(
        UdpSocket::bind(&local_addr)
            .await
            .expect("Failed to bind UDP socket when starting node"),
    );

    // Fire and forget gossip tasks
    let sending = tokio::spawn(send_gossip_on_interval(
        local_addr.clone(),
        socket.clone(),
        node_state.clone(),
        env.clone(),
    ));
    let receiving = tokio::spawn(receive_gossip(
        socket.clone(),
        node_state.clone(),
        env.clone(),
    ));

    // Initiate HTTP server
    info!("node={}; Starting Web Server: {}", &node_address, &web_addr);
    let web_server = tokio::spawn(web::web_server(
        web_addr.clone(),
        socket.clone(),
        node_state.clone(),
        env.clone(),
    ));

    // Panic if one of the tasks fails
    try_join!(sending, receiving, web_server)
        .map_err(|e| NodeError::ErrorStartingNode(format!("Node task failed: {}", e)))?;

    Ok(())
}

pub type NodeId = String;
pub type Peers = HashMap<NodeId, Node>;
pub type PartitionCount = HashMap<NodeId, HashMap<PartitionId, usize>>;

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
    pub partition_counts: HashMap<NodeId, HashMap<PartitionId, usize>>,
    pub node_hlc: HLC,
    // Stores the items that need to be gossiped to other nodes
    pub items_delta_state: HashMap<String, DeltaAckState>,
    // Stores the origin of the delta item to avoid sending it back to the same node that sent it
    pub items_delta_origin_cache: TtlCache<String, DeltaOrigin>,
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

/// Store acknowledgments for each item and the nodes it was sent to
#[derive(Debug, Clone)]
pub struct DeltaAckState {
    pub peers_pending: HashSet<NodeId>,
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
                partition_counts: HashMap::new(),
                address: local_addr,
                web_port: local_web_port,
                node_hlc: HLC {
                    timestamp: 0, // Initialize HLC with zero timestamp otherwise new node will be seen as source of truth for cluster state
                    counter: 0,
                },
                items_delta_state: HashMap::new(),
                items_delta_origin_cache: TtlCache::new(DELTA_ORIGIN_CACHE_INIT_CAPACITY),
                next_node_index: 0,
            };
            node.partition_map
                .assign(&node.all_peers.keys().cloned().collect::<Vec<_>>());
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
            panic!(
                "NodeState must be initialized with either a cluster config or a seed peer address"
            );
        }
    }

    pub fn get_address(&self) -> &String {
        match self {
            NodeState::Joined(joined_node) => &joined_node.address,
            NodeState::JoinedSyncing(joined_node) => &joined_node.address,
            NodeState::PreJoin(pre_join_node) => &pre_join_node.address,
            NodeState::Disconnected(disconnected_node) => &disconnected_node.address,
        }
    }
}

impl DisconnectedNode {
    pub fn get_this_node(&self) -> Node {
        Node {
            address: self.address.clone(),
            web_port: self.web_port,
            last_seen: self.node_hlc.timestamp,
        }
    }

    pub fn get_address(&self) -> &String {
        &self.address
    }
}

impl PreJoinNode {
    pub fn get_this_node(&self) -> Node {
        Node {
            address: self.address.clone(),
            web_port: self.web_port,
            last_seen: self.node_hlc.timestamp,
        }
    }

    pub fn get_address(&self) -> &String {
        &self.address
    }

    pub fn get_peer_node(&self) -> &String {
        &self.peer_node
    }

    pub async fn to_joined_state(
        &mut self,
        cluster_config: ClusterConfig,
        partition_map: PartitionMap,
        store: &dyn Store,
    ) -> Result<JoinedNode, NodeError> {
        let counts = store
            .partition_counts()
            .await
            .map_err(|e| NodeError::ErrorJoiningNode(e.to_string()))?;

        let mut partition_counts = HashMap::new();
        partition_counts.insert(self.address.clone(), counts);

        Ok(JoinedNode {
            address: self.address.clone(),
            web_port: self.web_port,
            all_peers: HashMap::new(),
            cluster_config,
            partition_map,
            partition_counts: partition_counts,
            node_hlc: self.node_hlc.clone(),
            items_delta_state: HashMap::new(),
            items_delta_origin_cache: TtlCache::new(DELTA_ORIGIN_CACHE_INIT_CAPACITY),
            next_node_index: 0,
        })
    }
}

impl JoinedNode {
    pub fn get_this_node(&self) -> Node {
        Node {
            address: self.address.clone(),
            web_port: self.web_port,
            last_seen: self.node_hlc.timestamp,
        }
    }

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
        info!(
            "node={}; Selected next node to gossip: {:?}",
            self.get_address(),
            &selected_peer
        );
        selected_peer
    }

    pub fn next_nodes(&mut self, count: u8) -> Vec<String> {
        let peer_size = self.other_peers().len();
        let max_count = min(count as usize, peer_size);
        let mut selected_next = vec![];
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

    pub fn get_node(&self, node_id: &str) -> Option<&Node> {
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
        self.node_hlc = self.node_hlc.tick_hlc(now_millis());
    }

    pub fn remove_node(&mut self, node_id: &str) {
        if self.all_peers.remove(node_id).is_some() {
            self.node_hlc = self.node_hlc.tick_hlc(now_millis());
        } else {
            error!(
                "node={}; Node {} not found in all peers",
                self.get_address(),
                node_id
            );
        }
    }

    /// Add single item, updating if it exists or is older
    async fn insert_item(
        &mut self,
        entry: ItemEntry,
        from_node: &str,
        store: &dyn Store,
    ) -> Result<Option<Item>, NodeError> {
        let storage_key_string = entry.storage_key.to_string();
        let partition = self
            .partition_map
            .hash_key(&entry.storage_key.partition_key.value());

        if !self
            .partition_map
            .contains_partition(&self.address, &partition)
        {
            error!(
                "node={}; Received an item {} for a partition {} that this node does not own, ignoring it.",
                self.get_address(),
                &storage_key_string,
                &partition
            );
            return Ok(None);
        }

        let maybe_existing_item = store
            .get(&partition, &entry.storage_key)
            .await
            .map_err(|e| NodeError::ItemOperationError(e.to_string()))?;

        if let Some(existing_entry) = maybe_existing_item {
            if entry.item.hlc > existing_entry.item.hlc {
                let new_item = Item {
                    message: entry.item.message.clone(),
                    status: entry.item.status.clone(),
                    hlc: HLC::merge(&existing_entry.item.hlc, &entry.item.hlc, now_millis()),
                };

                store
                    .insert(&partition, &entry.storage_key, new_item.clone())
                    .await
                    .map_err(|e| NodeError::ItemOperationError(e.to_string()))?;

                info!(
                    "node={}; Updated item {} with new entry: {:?}",
                    self.get_address(),
                    entry.storage_key.to_string(),
                    &new_item
                );

                // Update the delta and cache
                self.items_delta_origin_cache.insert(
                    storage_key_string.clone(),
                    DeltaOrigin {
                        node_id: from_node.to_string(),
                        item_hlc: new_item.hlc.clone(),
                    },
                    Duration::from_secs(DELTA_ORIGIN_CACHE_TTL_SECS),
                );

                // Invalidate delta state as we have new state for the item
                // e.g. if we received an update for an item that is still pending acknowledgment we want to remove it from delta state
                self.invalidate_delta_state(&storage_key_string);

                Ok(Some(new_item.clone()))
            } else {
                // If the new entry is older we do nothing
                info!(
                    "node={}; Received an update for item {} that is older or equal to the existing entry, ignoring it. Incoming {:?} vs Existing {:?}",
                    self.get_address(),
                    &storage_key_string,
                    &entry.item.hlc,
                    &existing_entry.item.hlc
                );
                Ok(None)
            }
        } else {
            // Insert new item, note it may also be a tombstone item
            let new_entry = entry.clone();
            store
                .insert(&partition, &entry.storage_key, new_entry.item.clone())
                .await
                .map_err(|e| NodeError::ItemOperationError(e.to_string()))?;

            info!(
                "node={}; Inserted new item {}",
                self.get_address(),
                &storage_key_string
            );

            // Add to delta cache
            self.items_delta_origin_cache.insert(
                storage_key_string.clone(),
                DeltaOrigin {
                    node_id: from_node.to_string(),
                    item_hlc: new_entry.item.hlc.clone(),
                },
                Duration::from_secs(DELTA_ORIGIN_CACHE_TTL_SECS),
            );
            self.invalidate_delta_state(&storage_key_string);

            Ok(Some(new_entry.item.clone()))
        }
    }

    /// Add multiple items, updating if they exist and are older
    ///
    /// This also includes Tombstone items coming from remove nodes. Local deletions are handle via remove_item.
    pub async fn insert_items(
        &mut self,
        items: &[ItemEntry],
        from_node: &str,
        store: &dyn Store,
    ) -> Result<Vec<Item>, NodeError> {
        let mut added_items = vec![];

        // Track the max HLC observed in this batch so we can update node_hlc
        let mut max_seen_hlc = self.node_hlc.clone();

        for item in items {
            let maybe_new_entry = self
                .insert_item(item.clone(), from_node, store)
                .await
                .map_err(|e| NodeError::ItemOperationError(e.to_string()))?;

            // Update max_seen_hlc with the incoming item's HLC
            if item.item.hlc > max_seen_hlc {
                max_seen_hlc = item.item.hlc.clone();
            }

            if let Some(new_entry) = maybe_new_entry {
                added_items.push(new_entry.clone());
            }
        }
        let this_node = self.get_address().clone();

        let partition_counts = match store.partition_counts().await {
            Ok(counts) => counts,
            Err(e) => {
                error!("node={}; Failed to get partition counts: {}", this_node, e);
                HashMap::new()
            }
        };
        self.update_partition_counts(&this_node, partition_counts);

        // Merge observed HLCs into node_hlc so the node's clock reflects received state.
        self.node_hlc = HLC::merge(&self.node_hlc, &max_seen_hlc, now_millis());
        Ok(added_items)
    }

    pub async fn add_local_item(
        &mut self,
        storage_key: &StorageKey,
        message: Vec<u8>,
        from_node: &str,
        env: Arc<Env>,
    ) -> Result<Item, NodeError> {
        let advanced_item_hlc = HLC::merge_and_tick(&self.node_hlc, &HLC::new(), now_millis());
        // since this is local event we want to update node_hlc
        self.node_hlc = advanced_item_hlc.clone();

        let item = Item {
            message: message.clone(),
            status: ItemStatus::Active,
            hlc: advanced_item_hlc.clone(),
        };

        let new_entry = ItemEntry {
            storage_key: storage_key.clone(),
            item: item.clone(),
        };

        let result = self
            .insert_items(&vec![new_entry], from_node, env.get_store())
            .await?;

        env.get_event_publisher()
            .publish(&crate::event::Event {
                address_from: self.get_address().clone(),
                address_to: self.get_address().clone(),
                message_type: "LocalItemAdded".to_string(),
                data: serde_json::json!({
                    "item_id": storage_key.to_string(),
                    "inserted_count": result.len(),
                }),
                timestamp: now_millis(),
            })
            .await;

        if let Some(added_item) = result.into_iter().next() {
            Ok(added_item)
        } else {
            Err(NodeError::ItemOperationError(
                "Failed to add local item".to_string(),
            ))
        }
    }

    /// Remove item by marking it as Tombstone
    ///
    /// This is used for local deletions. Tombstone items received from other nodes are handled via add_items.
    pub async fn remove_local_item(
        &mut self,
        storage_key: &StorageKey,
        from_node: &str,
        env: Arc<Env>,
    ) -> Result<bool, NodeError> {
        let store = env.get_store();
        let partition = self
            .partition_map
            .hash_key(&storage_key.partition_key.value());

        let maybe_existing_entry = store
            .get(&partition, storage_key)
            .await
            .map_err(|e| NodeError::ItemOperationError(e.to_string()))?;

        let result = if let Some(existing_entry) = maybe_existing_entry {
            let advanced_item_hlc = HLC::merge_and_tick(&self.node_hlc, &HLC::new(), now_millis());
            // since this is local event we want to update node_hlc
            self.node_hlc = advanced_item_hlc.clone();
            let item = Item {
                message: existing_entry.item.message.clone(),
                status: ItemStatus::Tombstone(now_millis()),
                hlc: advanced_item_hlc.clone(),
            };

            let new_entry = ItemEntry {
                storage_key: storage_key.clone(),
                item: item.clone(),
            };

            let result = self
                .insert_items(&vec![new_entry], from_node, env.get_store())
                .await?;

            env.get_event_publisher()
                .publish(&crate::event::Event {
                    address_from: self.get_address().clone(),
                    address_to: self.get_address().clone(),
                    message_type: "LocalItemRemoved".to_string(),
                    data: serde_json::json!({
                        "item_id": storage_key.to_string(),
                        "removed_count": result.len(),
                    }),
                    timestamp: now_millis(),
                })
                .await;
            Some(result)
        } else {
            None // Item not found
        };

        match result {
            Some(items) if !items.is_empty() => Ok(true),
            _ => Ok(false),
        }
    }

    /// Purge delta state for acknowledged delta items
    ///
    /// The reason why we check each item insead of a batch of items as a whole is that we want to ensure
    /// that we only remove the items that are acknowledged by the peer and not remove items that are still pending acknowledgment,
    /// e.g. in case new itmes are added to the delta state.
    pub async fn purge_delta_state(
        &mut self,
        from_node: &str,
        ack_delta_items: &[ItemEntry],
        store: &dyn Store,
    ) -> Result<(), NodeError> {
        for ack_item in ack_delta_items {
            let storage_key_string = ack_item.storage_key.to_string();
            if let Some(delta_ack) = self.items_delta_state.get_mut(&storage_key_string) {
                delta_ack.peers_pending.remove(from_node);

                if delta_ack.peers_pending.is_empty() {
                    info!(
                        "node={}; removing item {} from delta state as all peers have acknowledged it",
                        self.get_address(),
                        &storage_key_string
                    );
                    store
                        .remove_from_delta(&ack_item.storage_key)
                        .await
                        .map_err(|e| NodeError::ItemOperationError(e.to_string()))?;

                    self.items_delta_state.remove(&storage_key_string);
                }
            }
        }
        Ok(())
    }

    /// Add new items to delta state
    ///
    /// Used when delta is sent to other nodes to track which nodes have yet to acknowledge the item
    pub fn add_delta_state(&mut self, items: &[ItemEntry], delta_ack_state: DeltaAckState) {
        for item in items {
            let storage_key_string = item.storage_key.to_string();
            if let Some(delta_ack) = self.items_delta_state.get_mut(&storage_key_string) {
                delta_ack
                    .peers_pending
                    .extend(delta_ack_state.peers_pending.clone());
                delta_ack.created_at = now_millis();
            } else {
                self.items_delta_state
                    .insert(storage_key_string.clone(), delta_ack_state.clone());
            }
            info!(
                "node={}; Added to delta state for item {}: {:?}",
                self.get_address(),
                storage_key_string,
                &delta_ack_state
            );
        }
    }

    /// Expire state if we received new state for the item
    pub fn invalidate_delta_state(&mut self, item_id: &str) {
        self.items_delta_state.remove(item_id);
        info!(
            "node={}; Invalidated delta state for item {}",
            self.get_address(),
            &item_id
        );
    }

    /// Remove items from delta state after some time
    pub fn cleanup_expired_delta_state(&mut self) {
        let now = now_millis();
        let peers: std::collections::HashSet<_> = self.all_peers.keys().cloned().collect();

        self.items_delta_state.retain(|_item_id, delta_ack| {
            let age = now.saturating_sub(delta_ack.created_at) as u128;
            if age > DELTA_STATE_EXPIRY.as_millis()
                && delta_ack
                    .peers_pending
                    .iter()
                    .all(|peer| !peers.contains(peer))
            {
                false
            } else {
                true
            }
        });
    }

    pub async fn clear_delta(
        &mut self,
        delta_ids: &[StorageKey],
        store: &dyn Store,
    ) -> Result<(), NodeError> {
        if delta_ids.is_empty() {
            store
                .clear_all_delta()
                .await
                .map_err(|e| NodeError::ItemOperationError(e.to_string()))?;
        } else {
            for storage_key in delta_ids {
                store
                    .remove_from_delta(storage_key)
                    .await
                    .map_err(|e| NodeError::ItemOperationError(e.to_string()))?;
            }
        }
        Ok(())
    }

    /// Create a list of deltas that has to be send to a given node
    ///
    /// This excludes items that the node already has based on the delta cache
    pub async fn get_delta_for_node(
        &self,
        node: &NodeId,
        store: &dyn Store,
    ) -> Result<Vec<ItemEntry>, NodeError> {
        let mut items = vec![];

        let item_entries = store
            .get_all_delta()
            .await
            .map_err(|e| NodeError::ItemOperationError(e.to_string()))?;

        for item_entry in item_entries.iter() {
            // check if the node is the owner of the partition item should belong to
            let partition = self
                .partition_map
                .hash_key(&item_entry.storage_key.partition_key.value());
            if !self.partition_map.contains_partition(node, &partition) {
                continue;
            }

            let storage_key_string = item_entry.storage_key.to_string();
            if let Some(cached_item) = self.items_delta_origin_cache.get(&storage_key_string) {
                if cached_item.node_id == *node && cached_item.item_hlc < item_entry.item.hlc {
                    items.push(item_entry.clone());
                } else if cached_item.node_id != *node {
                    items.push(item_entry.clone());
                }
            } else {
                items.push(item_entry.clone());
            }
        }
        Ok(items)
    }

    /// Updates it's peers with peers from upstream
    ///
    /// # Arguments
    ///
    /// * `joining_cluster` - whether or not we need to update partition map after new node joined the cluster
    ///
    pub fn update_cluster_membership(
        &mut self,
        from: &NodeId,
        peers: &HashMap<String, Node>,
        partition_map: &PartitionMap,
        joining_cluster: bool,
    ) {
        let mut new_peers = peers.clone();
        new_peers.entry(from.to_string()).and_modify(|node| {
            node.last_seen = now_millis();
        });

        for (addr, node) in new_peers.iter() {
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
            self.partition_map
                .assign(&self.all_peers.keys().cloned().collect::<Vec<_>>());
            self.node_hlc = self.node_hlc.tick_hlc(now_millis());
        } else if !partition_map.is_empty() {
            self.partition_map = partition_map.clone();
        }
    }

    pub async fn get_item(
        &self,
        store_key: &StorageKey,
        store: &dyn Store,
    ) -> Result<Option<ItemEntry>, NodeError> {
        let partition = self
            .partition_map
            .hash_key(&store_key.partition_key.value());
        let maybe_item = store
            .get(&partition, store_key)
            .await
            .map_err(|e| NodeError::ItemOperationError(e.to_string()))?;

        Ok(maybe_item)
    }

    pub async fn get_items(
        &self,
        limit: usize,
        store_key: &StorageKey,
        store: &dyn Store,
    ) -> Result<Vec<ItemEntry>, NodeError> {
        let partition = self
            .partition_map
            .hash_key(&store_key.partition_key.value());
        let maybe_item = store
            .get_many(&partition, store_key, limit)
            .await
            .map_err(|e| NodeError::ItemOperationError(e.to_string()))?;

        Ok(maybe_item)
    }

    pub fn items_count(&self) -> usize {
        let mut unique_partitions = HashMap::<PartitionId, usize>::new();
        for node_partitions in self.partition_counts.iter() {
            for node_partition in node_partitions.1.iter() {
                unique_partitions
                    .entry(*node_partition.0)
                    .or_insert(*node_partition.1);
            }
        }

        unique_partitions.values().sum()
    }

    pub fn update_partition_counts(
        &mut self,
        node_id: &str,
        partition_counts: HashMap<PartitionId, usize>,
    ) {
        self.partition_counts
            .insert(node_id.to_string(), partition_counts);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::HLC;
    use crate::item::{Item, ItemStatus};
    use crate::store::PartitionKey;
    use crate::store::memory_store::InMemoryStore;

    #[tokio::test]
    async fn test_reconcile_delta() {
        let node_a = "127.0.0.1:1000".to_string();
        let node_b = "127.0.0.1:1001".to_string();

        let store: Box<dyn Store> = Box::new(InMemoryStore::new());
        let cluster_config = ClusterConfig {
            cluster_size: 2,
            partition_count: 8,
            replication_factor: 2,
        };

        let mut node_state = NodeState::init(
            node_a.clone(),
            3001,
            Some(node_b.clone()),
            Some(cluster_config),
        );

        let item1 = ItemEntry {
            storage_key: StorageKey::new(PartitionKey("123".to_string()), None),
            item: Item {
                message: "item1 message".as_bytes().to_vec(),
                status: ItemStatus::Active,
                hlc: HLC {
                    timestamp: 100,
                    counter: 0,
                },
            },
        };

        match &mut node_state {
            NodeState::Joined(joined_node) => {
                // Add item to store
                let _ = joined_node
                    .insert_items(&vec![item1.clone()], &node_a, &*store)
                    .await;

                assert_eq!(joined_node.items_count(), 1);

                // Simulate sending delta to nodeA
                joined_node.add_delta_state(
                    &[item1.clone()],
                    DeltaAckState {
                        peers_pending: [node_a.clone(), node_b.clone()].iter().cloned().collect(),
                        created_at: now_millis(),
                    },
                );

                // Check that delta state is added
                assert_eq!(joined_node.items_delta_state.len(), 1);

                // Simulate receiving acknowledgment from nodeA only
                let _ = joined_node
                    .purge_delta_state(&node_a, &[item1.clone()], &*store)
                    .await;

                // Check that delta state is cleared for nodeA but still pending for nodeB
                let new_deltas_for_node_a = joined_node
                    .get_delta_for_node(&node_a, &*store)
                    .await
                    .unwrap();
                let new_deltas_for_node_b = joined_node
                    .get_delta_for_node(&node_b, &*store)
                    .await
                    .unwrap();

                assert_eq!(new_deltas_for_node_a.len(), 0);
                assert_eq!(new_deltas_for_node_b.len(), 1);

                // Verify delta state still has the item pending for nodeB
                assert_eq!(joined_node.items_delta_state.len(), 1);

                // Simulate receiving acknowledgment from nodeA only and delta state should be cleared now
                let _ = joined_node
                    .purge_delta_state(&node_b, &[item1.clone()], &*store)
                    .await;
                let new_deltas_for_node_b = joined_node
                    .get_delta_for_node(&node_b, &*store)
                    .await
                    .unwrap();
                assert_eq!(new_deltas_for_node_b.len(), 0);
            }
            _ => panic!("Node is not in Joined state"),
        }
    }
}
