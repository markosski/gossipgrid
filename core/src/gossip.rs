use crate::item::{ItemEntry, ItemStatus};
use crate::partition::PartitionMap;
use bincode::{Decode, Encode};
use log::{error, info};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::time::sleep;

const GOSSIP_INTERVAL: Duration = Duration::from_secs(5);
const FAILURE_TIMEOUT: Duration = Duration::from_secs(15);
const BUFFER_SIZE: usize = 1024;

#[derive(Debug, Clone, Encode, Decode, Ord, PartialOrd, PartialEq, Eq)]
pub struct HLC {
    pub timestamp: u64,
    pub counter: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Node {
    pub address: String,
    pub last_seen: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct SyncRequest {
    pub is_sync_request: bool,
    pub since: HLC,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct GossipMessage {
    pub cluster_config: ClusterConfig,
    pub partition_map: PartitionMap,
    pub all_peers: HashMap<String, Node>,
    pub node_hlc: HLC,
    pub items_delta: Vec<ItemEntry>,
    pub sync_request: SyncRequest,
    pub sync_response: bool,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct ClusterConfig {
    pub cluster_size: usize,
    pub partition_count: usize,
    pub replication_factor: usize,
}

pub struct NodeMemory {
    pub this_node: String,
    pub cluster_config: ClusterConfig,
    pub partition_map: PartitionMap,
    pub all_peers: HashMap<String, Node>,
    pub node_hlc: HLC,
    pub items: HashMap<String, ItemEntry>,
    pub index: BTreeMap<HLC, HashSet<String>>,
    pub node_index: u8,
}

impl NodeMemory {
    pub fn init(
        local_addr: String,
        seed_peer: Option<String>,
        cluster_config: ClusterConfig,
    ) -> NodeMemory {
        let mut known_peers = HashMap::new();
        known_peers.insert(
            local_addr.clone(),
            Node {
                address: local_addr.clone(),
                last_seen: 0,
            },
        );

        if let Some(peer) = seed_peer {
            known_peers.insert(
                peer.clone(),
                Node {
                    address: peer.clone(),
                    last_seen: now_millis(),
                },
            );
        }

        NodeMemory {
            this_node: local_addr,
            cluster_config: cluster_config.clone(),
            partition_map: PartitionMap::new(
                &cluster_config.partition_count,
                &cluster_config.replication_factor,
            ),
            all_peers: known_peers,
            node_hlc: HLC {
                timestamp: now_millis(),
                counter: 0,
            },
            items: HashMap::new(),
            index: BTreeMap::new(),
            node_index: 0,
        }
    }

    pub fn next_node(&mut self) -> String {
        let other_peers = self.other_peers();
        let mut nodes: Vec<String> = other_peers.keys().cloned().collect();
        nodes.sort();

        let selected_peer = nodes
            .get(self.node_index as usize % other_peers.len())
            .cloned()
            .unwrap_or_else(|| self.this_node.clone());

        if self.node_index < self.other_peers().len() as u8 {
            self.node_index += 1;
        } else {
            self.node_index = 0;
        }
        selected_peer
    }

    pub fn size(&self) -> usize {
        self.all_peers.len()
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
            self.items.remove(node_id);
            self.node_hlc.tick_hlc(now_millis());
        }
    }

    pub fn add_item(&mut self, entry: ItemEntry) {
        if self.items.contains_key(&entry.item.id) {
            // Update existing item
            if let Some(new_entry) = self.items.get_mut(&entry.item.id) {
                match new_entry.status {
                    ItemStatus::Active => {
                        // Update the item
                        if new_entry.hlc.compare(&entry.hlc) == Ordering::Less {
                            new_entry.hlc = HLC::merge(&new_entry.hlc, &entry.hlc, now_millis());
                            new_entry.item = entry.item;

                            match self.index.get_mut(&new_entry.hlc) {
                                Some(set) => {
                                    // If the set already exists, we just add the item id
                                    set.insert(new_entry.item.id.clone());
                                }
                                None => {
                                    // If the set does not exist, we create a new one
                                    let mut set = HashSet::new();
                                    set.insert(new_entry.item.id.clone());
                                    self.index.insert(new_entry.hlc.clone(), set);
                                }
                            }
                        }
                    }
                    ItemStatus::Tombstone(_) => {
                        // If it was a tombstone, we do nothing
                        // TODO: maybe we should remove it from the index?
                    }
                };
            }
        } else {
            // Add new item
            let mut new_entry = entry.clone();
            new_entry.status = ItemStatus::Active;
            new_entry.hlc = new_entry.hlc.tick_hlc(now_millis());
            self.items.insert(entry.item.id.clone(), new_entry.clone());

            let mut set = HashSet::new();
            set.insert(new_entry.item.id.clone());
            self.index.insert(new_entry.hlc.clone(), set);
        }
    }

    pub fn add_items(&mut self, items: Vec<ItemEntry>) {
        for item in items {
            self.add_item(item);
        }
    }

    pub fn remove_item(&mut self, item_id: &String) -> bool {
        if let Some(item_entry) = self.items.get_mut(item_id) {
            let mut new_item_entry = item_entry.clone();
            new_item_entry.status = ItemStatus::Tombstone(now_millis());

            self.items.insert(item_id.clone(), new_item_entry);
            return true;
        } else {
            return false; // Node not found
        }
    }

    pub fn count(&self) -> usize {
        self.items.len()
    }

    pub fn items_since(&self, hlc: &HLC) -> Vec<ItemEntry> {
        let mut items = vec![];
        for (_, set) in self.index.range(hlc..).rev() {
            for item_id in set {
                if let Some(item) = self.items.get(item_id) {
                    items.push(item.clone());
                }
            }
        }
        items
    }
}

impl HLC {
    pub fn new() -> HLC {
        HLC {
            timestamp: 0,
            counter: 0,
        }
    }

    pub fn compare(&self, other: &HLC) -> Ordering {
        match self.timestamp.cmp(&other.timestamp) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => self.counter.cmp(&other.counter),
        }
    }

    pub fn tick_hlc(&self, now: u64) -> HLC {
        if now > self.timestamp {
            HLC {
                timestamp: now,
                counter: 0,
            }
        } else {
            HLC {
                timestamp: self.timestamp,
                counter: self.counter + 1,
            }
        }
    }

    pub fn merge(local: &HLC, remote: &HLC, now: u64) -> HLC {
        let merged_pt = local.timestamp.max(remote.timestamp).max(now);

        let merged_lc = if merged_pt == local.timestamp && merged_pt == remote.timestamp {
            local.counter.max(remote.counter) + 1
        } else if merged_pt == local.timestamp {
            local.counter + 1
        } else if merged_pt == remote.timestamp {
            remote.counter + 1
        } else {
            0 // wall clock moved forward
        };

        HLC {
            timestamp: merged_pt,
            counter: merged_lc,
        }
    }
}

impl RangeBounds<HLC> for HLC {
    fn start_bound(&self) -> Bound<&HLC> {
        Bound::Included(self)
    }

    fn end_bound(&self) -> Bound<&HLC> {
        Bound::Included(self)
    }
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

pub async fn receive_gossip(
    node_name: String,
    socket: Arc<UdpSocket>,
    memory: Arc<Mutex<NodeMemory>>,
    sync_flag: Arc<Mutex<bool>>,
) {
    let mut buf = [0u8; BUFFER_SIZE];

    loop {
        match socket.recv_from(&mut buf).await {
            Ok((size, src)) => {
                let (response, _) = bincode::decode_from_slice::<GossipMessage, _>(
                    &buf[..size],
                    bincode::config::standard(),
                )
                .unwrap();
                info!(
                    "node={}; received {:?} from {:?}:{:?}",
                    node_name,
                    response,
                    src.ip(),
                    src.port()
                );

                {
                    let mut memory = memory.lock().await;
                    let local_addr = memory.this_node.clone();

                    // Update peers to most recent timestamp and vector clock
                    match response.node_hlc.compare(&memory.node_hlc) {
                        Ordering::Greater => {
                            memory.node_hlc =
                                HLC::merge(&memory.node_hlc, &response.node_hlc, now_millis());
                            memory.all_peers = response.all_peers.clone();
                        }
                        Ordering::Equal | Ordering::Less => {
                            info!(
                                "node={}; Received HLC is older or equal to local HLC",
                                node_name
                            );
                            // Either local is up-to-date or newer; reject the membership update
                            // but you may still want to merge tasks separately later!
                        }
                    }

                    if memory.partition_map.is_empty()
                        && memory.size() == memory.cluster_config.cluster_size
                    {
                        // Check if we need to update the partition map
                        let mut partition_map = memory.partition_map.clone();
                        partition_map.assign(&memory.all_peers.keys().cloned().collect::<Vec<_>>());
                        info!(
                            "************************ {:?}, {:?}",
                            &partition_map,
                            &memory.all_peers.keys().cloned().collect::<Vec<_>>()
                        );
                        memory.partition_map = partition_map;
                    }

                    // update tasks
                    let items = response.items_delta.clone();
                    memory.add_items(items);

                    if response.sync_request.is_sync_request && memory.size() > 1 {
                        info!("node={}; Syncing back to {}", node_name, &src);
                        let mut items: Vec<ItemEntry> = vec![];
                        let last_seen = &memory.all_peers.get(&src.to_string()).unwrap().last_seen;
                        let last_see_hlc = HLC::new().tick_hlc(last_seen.clone());
                        for entry in memory.items_since(&last_see_hlc) {
                            if let Some(e) = memory.items.get(&entry.item.id) {
                                items.push(e.clone());
                            }
                        }
                        info!(
                            "node={}; Sync identifier {} items to send since {}",
                            node_name,
                            &items.len(),
                            last_seen
                        );
                        send_gossip_single(
                            &items,
                            Some(&src.to_string()),
                            &local_addr,
                            socket.clone(),
                            &mut memory,
                            true,
                        )
                        .await;
                    }

                    // bump last update time, it is important this happens after the sync
                    memory.all_peers.entry(src.to_string()).and_modify(|node| {
                        node.last_seen = now_millis();
                    });

                    if response.sync_response {
                        let mut sync = sync_flag.lock().await;
                        *sync = false; // TODO: understand how this works
                    }
                }
            }
            Err(e) => {
                error!("Receive error: {}", e);
            }
        }
    }
}

pub async fn send_gossip_single(
    items: &Vec<ItemEntry>,
    peer_addr: Option<&String>,
    local_addr: &String,
    socket: Arc<UdpSocket>,
    memory: &mut NodeMemory,
    is_sync: bool,
) {
    info!("node={}; Will send {:?} items", local_addr, &items.len());

    let peer_dest: Option<String>;
    if let Some(peer) = peer_addr {
        peer_dest = Some(peer.clone());
    } else if memory.size() > 1 {
        peer_dest = Some(memory.next_node().to_string());
    } else {
        peer_dest = None;
    }

    if let Some(peer_dest_actual) = peer_dest {
        let msg = GossipMessage {
            cluster_config: memory.cluster_config.clone(),
            partition_map: memory.partition_map.clone(),
            all_peers: memory.all_peers.clone(),
            node_hlc: memory.node_hlc.clone(),
            items_delta: items.clone(),
            sync_request: SyncRequest {
                is_sync_request: false,
                since: HLC::new(),
            },
            sync_response: is_sync,
        };

        let encoded = bincode::encode_to_vec(&msg, bincode::config::standard()).unwrap();
        socket.send_to(&encoded, &peer_dest_actual).await.unwrap();
        info!(
            "node={}; sent {:?} to {:?}; sync={}",
            &local_addr, &msg, &peer_dest_actual, &is_sync
        );
    } else {
        info!("node={}; no other peers to gossip with", &local_addr);
    }
}

pub async fn send_gossip(
    node_name: String,
    local_addr: String,
    socket: Arc<UdpSocket>,
    memory: Arc<Mutex<NodeMemory>>,
    sync_flag: Arc<Mutex<bool>>,
) {
    loop {
        // lock will be released after the block ends
        {
            let mut memory = memory.lock().await;
            let sync_flag = sync_flag.lock().await;
            let now_millis = now_millis();

            // Remove peers that haven't responded within FAILURE_TIMEOUT
            let peers = memory.all_peers.clone();
            for peer in peers.iter() {
                if peer.0 != &local_addr
                    && now_millis - peer.1.last_seen > FAILURE_TIMEOUT.as_millis() as u64
                {
                    memory.remove_node(peer.0);
                    info!(
                        "node={}; Removed peer: {:?} from known peers",
                        node_name, peer.0
                    );
                }
            }

            let other_peers = memory.other_peers();
            info!("node={}; Known peers: {:?}", node_name, &other_peers);

            // Check if we need to send gossip
            if memory.size() > 1 {
                let peer_dest = memory.next_node().to_string();
                info!("node={}; selected node: {:?}", node_name, &peer_dest);

                // Send gossip to a random peer
                let vec: Vec<String> = other_peers.keys().cloned().collect();
                if vec.len() > 0 {
                    let index = rand::random_range(0..vec.len());
                    if let Some(peer) = vec.get(index) {
                        let msg = GossipMessage {
                            cluster_config: memory.cluster_config.clone(),
                            partition_map: memory.partition_map.clone(),
                            all_peers: peers,
                            node_hlc: memory.node_hlc.clone(),
                            items_delta: vec![],
                            sync_request: SyncRequest {
                                is_sync_request: sync_flag.clone(),
                                since: HLC::new(),
                            },
                            sync_response: false,
                        };
                        let encoded =
                            bincode::encode_to_vec(&msg, bincode::config::standard()).unwrap();

                        socket.send_to(&encoded, &peer_dest).await.unwrap();
                        info!(
                            "node={}; sent {:?} to {:?}; sync={}",
                            node_name, msg, &peer_dest, &sync_flag
                        );
                    }
                }
            }
        }
        sleep(GOSSIP_INTERVAL).await;
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::item::Item;

//     #[test]
//     fn hlc_compare() {
//         let hlc1 = HLC { timestamp: 100, counter: 0 };
//         let hlc2 = HLC { timestamp: 200, counter: 0 };
//         let hlc3 = HLC { timestamp: 100, counter: 1 };
//         let hlc4 = HLC { timestamp: 100, counter: 0 };

//         assert_ne!(hlc1, hlc3);
//         assert_eq!(hlc1, hlc4);
//         assert_eq!(hlc1.compare(&hlc2), Ordering::Less);
//         assert_eq!(hlc2.compare(&hlc1), Ordering::Greater);
//         assert_eq!(hlc1.compare(&hlc3), Ordering::Less);
//         assert_eq!(hlc3.compare(&hlc1), Ordering::Greater);
//     }

//     #[test]
//     fn test_next_node() {
//         let mut memory = NodeMemory::init("127.0.0.1:1000".to_string(), None, now_millis());
//         memory.add_node(Node { address: "127.0.0.1:1200".to_string(), last_seen: 100 });
//         memory.add_node(Node { address: "127.0.0.1:1300".to_string(), last_seen: 100 });
//         memory.add_node(Node { address: "127.0.0.1:1400".to_string(), last_seen: 100 });

//         let next_node = vec!(memory.next_node(), memory.next_node(), memory.next_node(), memory.next_node());
//         assert_eq!(next_node, vec!("127.0.0.1:1200", "127.0.0.1:1300", "127.0.0.1:1400", "127.0.0.1:1200"));
//     }

//     #[test]
//     fn test_merge_tasks() {
//         let mut memory = NodeMemory::init("127.0.0.1:1000".to_string(), None, now_millis());
//         memory.items.insert("task1".to_string(), ItemEntry {
//             item: Item {
//                 id: "task1".to_string(),
//                 message: "task1 message".to_string(),
//                 submitted_at: 100,
//             },
//             status: ItemStatus::Active,
//             hlc: HLC { timestamp: 100, counter: 0 },
//         });
//         memory.items.insert("task2".to_string(), ItemEntry {
//             item: Item {
//                 id: "task2".to_string(),
//                 message: "task1 message".to_string(),
//                 submitted_at: 100,
//             },
//             status: ItemStatus::Active,
//             hlc: HLC { timestamp: 200, counter: 0 },
//         });
//         memory.items.insert("task3".to_string(), ItemEntry {
//             item: Item {
//                 id: "task3".to_string(),
//                 message: "task1 message".to_string(),
//                 submitted_at: 100,
//             },
//             status: ItemStatus::Active,
//             hlc: HLC { timestamp: 300, counter: 0 },
//         });

//         memory.index.insert(HLC { timestamp: 100, counter: 0 }, HashSet::from(["task1".to_string()]));
//         memory.index.insert(HLC { timestamp: 200, counter: 0 }, HashSet::from(["task2".to_string()]));
//         memory.index.insert(HLC { timestamp: 300, counter: 0 }, HashSet::from(["task3".to_string()]));

//         let items = memory.items_since(&HLC { timestamp: 150, counter: 0 });
//         assert_eq!(memory.index.len(), 3);
//         assert_eq!(memory.items.len(), 3);
//         assert_eq!(items.len(), 2);

//     }
// }
