use crate::item::{ItemEntry, ItemStatus};
use crate::node::{ClusterConfig, DeltaAckState, Node, NodeMemory};
use crate::partition::PartitionMap;
use crate::{gossip, now_millis};
use bincode::{Decode, Encode};
use log::{debug, error, info};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::time::sleep;

const GOSSIP_DELTA_TIMEOUT: Duration = Duration::from_secs(30);
const GOSSIP_INTERVAL: Duration = Duration::from_secs(5);
const FAILURE_TIMEOUT: Duration = Duration::from_secs(15);
const BUFFER_SIZE: usize = 1024;

#[derive(Debug, Clone, Encode, Decode, Ord, PartialOrd, PartialEq, Eq)]
pub struct HLC {
    pub timestamp: u64,
    pub counter: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct SyncRequest {
    pub is_sync_request: bool,
    pub since: HLC,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct GossipAck {
    pub items_received: Vec<ItemEntry>,
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

pub async fn send_gossip_ack(
    node_name: &str,
    items: &[ItemEntry],
    peer_addr: &String,
    local_addr: &String,
    socket: Arc<UdpSocket>
) {
    info!("node_addr={}; Sending ACK {:?} items", node_name, &items.len());

    let ack = GossipAck {
        items_received: items.to_vec(),
    };
    let encoded_ack = bincode::encode_to_vec(&ack, bincode::config::standard()).unwrap();
    socket.send_to(&encoded_ack, &peer_addr).await.unwrap();
    info!(
        "node_addr={}; sent {:?} to {:?}",
        &local_addr, &ack, &peer_addr
    );
}

async fn handle_main_message(node_name: &str, src: &SocketAddr, socket: Arc<UdpSocket>, sync_flag: Arc<Mutex<bool>>, message: GossipMessage, memory: Arc<Mutex<NodeMemory>>) {
    let mut memory = memory.lock().await;
    let local_addr = memory.this_node.clone();

    // Update peers to most recent timestamp and vector clock
    match message.node_hlc.compare(&memory.node_hlc) {
        Ordering::Greater | Ordering::Equal => {
            let membership_changed = memory.all_peers != message.all_peers;
            let partition_map_changed = memory.partition_map != message.partition_map;
            let config_changed = memory.cluster_config != message.cluster_config;

            if membership_changed || partition_map_changed || config_changed {
                memory.node_hlc =
                    HLC::merge(&memory.node_hlc, &message.node_hlc, now_millis());
                memory.take_peers(&message.all_peers);


                memory.partition_map = message.partition_map.clone();
                memory.cluster_config = message.cluster_config.clone();
                info!(
                    "node={}; Received HLC is newer or equal to local HLC; Updated node_hlc: {:?}",
                    node_name, memory.node_hlc
                );
            }
        }
        Ordering::Less => {
            if message.node_hlc.timestamp == 0 {
                memory.node_hlc.tick_hlc(now_millis());
                memory.add_node(message.all_peers.get(&src.to_string()).unwrap().clone());
                info!("node={}; Adding new node", node_name);
            }
            info!(
                "node={}; Received HLC is older to local HLC",
                node_name
            );
        }
    }

    // Update partition map information. This should happen only once during cluster initialization
    if memory.partition_map.is_empty() || (memory.cluster_size() as usize) > message.all_peers.len() {
        // Check if we need to update the partition map
        let mut partition_map = memory.partition_map.clone();
        partition_map.assign(&memory.all_peers.keys().cloned().collect::<Vec<_>>());
        memory.partition_map = partition_map;
        memory.node_hlc.tick_hlc(now_millis());
    }

    // Update received items and acknowledge
    if message.items_delta.len() > 0 {
        let items = message.items_delta.clone();
        let added = memory.add_items(items.clone(), &src.to_string()).await;

        if added.iter().count() > 0 {
            info!(
                "node={}; Added {} items from {}",
                node_name, added.iter().count(), &src
            );
            send_gossip_ack(node_name, &items, &src.to_string(), &local_addr, socket.clone()).await;
        } else {
            debug!(
                "node={}; No new items added from {}",
                node_name, &src
            );
        }
    }

    // Data sync
    if message.sync_request.is_sync_request && memory.cluster_size() > 1 {
        info!("node={}; Syncing back to {}", node_name, &src);
        let mut items: Vec<ItemEntry> = vec![];
        let last_seen = &memory.all_peers.get(&src.to_string()).unwrap().last_seen;
        let last_seen_hlc = HLC::new().tick_hlc(last_seen.clone());
        for entry in memory.items_since(&last_seen_hlc).await {
            if let Some(e) = memory.get_item(&entry.item.id).await {
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
            node_name,
            &items,
            Some(&src.to_string()),
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

    if message.sync_response {
        let mut sync = sync_flag.lock().await;
        *sync = false; // TODO: understand how this works
    }
}

async fn handle_item_delta_message(src: &SocketAddr, message: GossipAck, memory: Arc<Mutex<NodeMemory>>) {
    let mut memory = memory.lock().await;
    memory.reconcile_delta_state(src.to_string(), &message.items_received).await;
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
                if let Ok((message, _)) = bincode::decode_from_slice::<GossipMessage, _>(
                    &buf[..size],
                    bincode::config::standard(),
                ) {
                    info!("node={}; Message Received {:?} from {:?}:{:?}", node_name, message, src.ip(), src.port());
                    handle_main_message(&node_name, &src, socket.clone(), sync_flag.clone(), message, memory.clone()).await;
                };

                if let Ok((message, _)) = bincode::decode_from_slice::<GossipAck, _>(
                    &buf[..size],
                    bincode::config::standard(),
                ) {
                    info!("node={}; Message Received {:?} from {:?}:{:?}", node_name, message, src.ip(), src.port());
                    handle_item_delta_message(&src, message, memory.clone()).await;
                };
            }
            Err(e) => {
                error!("Receive error: {}", e);
            }
        }
    }
}

pub async fn send_gossip_single(
    node_name: &str,
    items: &Vec<ItemEntry>,
    peer_addr: Option<&String>,
    socket: Arc<UdpSocket>,
    memory: &mut NodeMemory,
    is_sync: bool,
) {
    info!("node={}; Will send {} items", &node_name, &items.len());

    let mut peers: Vec<String> = vec!();
    if let Some(peer) = peer_addr {
        peers = vec!(peer.clone());
    } else if memory.cluster_size() > 1 {
        peers = memory.next_nodes(memory.gossip_count());
    }

    // Send gossip to next nodes
    send_gossip_helper(&node_name, &peers, memory, false, is_sync, &socket).await;
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
                        "node={}; Removed peer: {} from known peers",
                        &node_name, &peer.0
                    );
                }
            }

            // Expire delta state if needed
            memory.cleanup_expired_delta_state();

            // Send gossip to next nodes
            let gossip_count = memory.gossip_count();
            let next_nodes = memory.next_nodes(gossip_count);
            send_gossip_helper(&node_name, &next_nodes, &mut memory, sync_flag.clone(), false, &socket).await;

            info!("node={}; Node: {:?}", node_name, &memory.get_node(&local_addr));
            info!("node={}; Known peers: {:?}", node_name, &memory.other_peers());
            info!("node={}; Item count: {}", node_name, &memory.items_count().await);
            info!("node={}; Delta Cache size: {}", node_name, &memory.items_delta_cache.iter().count());
            info!("node={}; Delta State size: {}", node_name, &memory.items_delta_state.len());
            info!("node={}; Vnodes: {:?}", node_name, &memory.partition_map.get_vnodes_for_node(&local_addr));
            info!("node={}; PartitionMap: {:?}", node_name, &memory.partition_map);
            // info!("node={}; Delta Items: {:?}", node_name, &memory..len());
        }
        sleep(GOSSIP_INTERVAL).await;
    }
}

async fn send_gossip_helper(node_name: &str, next_nodes: &[String], memory: &mut NodeMemory, sync_flag: bool, is_sync_response: bool, socket: &UdpSocket) {
    let mut all_delta_count = 0;

    for peer_dest in next_nodes.iter() {
        let itmes_delta = memory.get_delta_for_node(&peer_dest).await;
        all_delta_count += itmes_delta.len();
        info!("node={}; selected node: {}; sending delta size: {}", node_name, &peer_dest, &itmes_delta.len());

        let msg = GossipMessage {
            cluster_config: memory.cluster_config.clone(),
            partition_map: memory.partition_map.clone(),
            all_peers: memory.all_peers.clone(),
            node_hlc: memory.node_hlc.clone(),
            items_delta: itmes_delta.clone(),
            sync_request: SyncRequest {
                is_sync_request: sync_flag,
                since: HLC::new(),
            },
            sync_response: is_sync_response,
        };
        let encoded =
            bincode::encode_to_vec(&msg, bincode::config::standard()).unwrap();

        socket.send_to(&encoded, &peer_dest).await.unwrap();

        if !sync_flag {
            memory.add_delta_state(&itmes_delta, DeltaAckState {
                peers_pending: vec!(peer_dest.clone()).into_iter().collect(),
                created_at: now_millis(),
            });

            // Remove delta items if everyone we know has received them
            if all_delta_count == 0 && next_nodes.len() > 0 {
                memory.clear_delta().await;
            }
        }

        info!(
            "node={}; sent {:?} to {:?}; sync={}",
            node_name, msg, &peer_dest, &sync_flag
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::item::Item;

    #[test]
    fn hlc_compare() {
        let hlc1 = HLC { timestamp: 100, counter: 0 };
        let hlc2 = HLC { timestamp: 200, counter: 0 };
        let hlc3 = HLC { timestamp: 100, counter: 1 };

        assert_ne!(hlc1, hlc3);
        assert_eq!(hlc1.compare(&hlc2), Ordering::Less);
        assert_eq!(hlc2.compare(&hlc1), Ordering::Greater);
        assert_eq!(hlc1.compare(&hlc3), Ordering::Less);
        assert_eq!(hlc3.compare(&hlc1), Ordering::Greater);
        assert_eq!(hlc1.compare(&hlc3), Ordering::Less);
    }

    // #[test]
    // fn test_next_node() {
    //     let cluster_config = ClusterConfig {
    //         cluster_size: 2,
    //         partition_count: 8,
    //         replication_factor: 2,
    //     };
    //     let mut memory = NodeMemory::init("127.0.0.1:1000".to_string(), 3001, None, cluster_config);
    //     memory.add_node(Node { address: "127.0.0.1:1200".to_string(), last_seen: 100 });
    //     memory.add_node(Node { address: "127.0.0.1:1300".to_string(), last_seen: 100 });
    //     memory.add_node(Node { address: "127.0.0.1:1400".to_string(), last_seen: 100 });

    //     let next_node = vec!(memory.next_node(), memory.next_node(), memory.next_node(), memory.next_node());
    //     assert_eq!(next_node, vec!("127.0.0.1:1200", "127.0.0.1:1300", "127.0.0.1:1400", "127.0.0.1:1200"));
    // }


    // #[test]
    // fn test_next_nodes() {
    //     let cluster_config = ClusterConfig {
    //         cluster_size: 2,
    //         partition_count: 8,
    //         replication_factor: 2,
    //     };
    //     let mut memory = NodeMemory::init("127.0.0.1:1000".to_string(), 3001, None, cluster_config);
    //     memory.add_node(Node { address: "127.0.0.1:1200".to_string(), last_seen: 100 });
    //     memory.add_node(Node { address: "127.0.0.1:1300".to_string(), last_seen: 100 });

    //     let next_nodes_1= memory.next_nodes(2);
    //     let next_nodes_2= memory.next_nodes(2);

    //     assert_eq!(next_nodes_1, vec!("127.0.0.1:1200", "127.0.0.1:1300"));
    //     assert_eq!(next_nodes_2, vec!("127.0.0.1:1200", "127.0.0.1:1300"));
    // }

//     #[test]
//     fn test_merge_tasks() {
//         let cluster_config = ClusterConfig {
//             cluster_size: 2,
//             partition_count: 8,
//             replication_factor: 2,
//         };
//         let mut memory = NodeMemory::init("127.0.0.1:1000".to_string(), None, cluster_config);
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
}
