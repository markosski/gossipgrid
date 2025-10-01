use crate::env::Env;
use crate::item::ItemEntry;
use crate::node::{self, ClusterConfig, DeltaAckState, JoinedNode, Node, NodeState, PreJoinNode};
use crate::now_millis;
use crate::partition::{PartitionMap, PartitionId};
use bincode::{Decode, Encode};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::time::sleep;

const GOSSIP_DELTA_TIMEOUT: Duration = Duration::from_secs(30);
const GOSSIP_INTERVAL: Duration = Duration::from_secs(5);
const FAILURE_TIMEOUT: Duration = Duration::from_secs(15);
const BUFFER_SIZE: usize = 1024;

#[derive(Debug, Clone, Encode, Decode, Ord, PartialOrd, PartialEq, Eq, Serialize, Deserialize)]
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
pub struct GossipJoinMessage {
    pub node: Node,
    pub peer_address: String,
    pub node_hlc: HLC,
    pub sync_request: SyncRequest,
    pub sync_response: bool,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct GossipMessage {
    pub cluster_config: ClusterConfig,
    pub partition_map: PartitionMap,
    pub partition_item_counts: HashMap<PartitionId, usize>,
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

pub async fn receive_gossip(
    socket: Arc<UdpSocket>,
    node_state: Arc<RwLock<NodeState>>,
    sync_flag: Arc<Mutex<bool>>,
    env: Arc<Env>,
) {
    let mut buf = [0u8; BUFFER_SIZE];

    loop {
        match socket.recv_from(&mut buf).await {
            Ok((size, src)) => {
                if let Ok((message, _)) = bincode::decode_from_slice::<GossipMessage, _>(
                    &buf[..size],
                    bincode::config::standard(),
                ) {
                    handle_main_message(
                        &src,
                        socket.clone(),
                        sync_flag.clone(),
                        message,
                        node_state.clone(),
                        env.clone(),
                    )
                    .await;
                } else if let Ok((message, _)) = bincode::decode_from_slice::<GossipJoinMessage, _>(
                    &buf[..size],
                    bincode::config::standard(),
                ) {
                    handle_join_message(
                        &src,
                        socket.clone(),
                        sync_flag.clone(),
                        message,
                        node_state.clone(),
                        env.clone(),
                    )
                    .await;
                } else if let Ok((message, _)) = bincode::decode_from_slice::<GossipAck, _>(
                    &buf[..size],
                    bincode::config::standard(),
                ) {
                    handle_item_delta_message(&src, message, node_state.clone(), env.clone()).await;
                };
            }
            Err(e) => {
                error!("Receive error: {}", e);
            }
        }
    }
}

pub async fn send_gossip_ack(
    node_name: &str,
    items: &[ItemEntry],
    peer_addr: &String,
    local_addr: &String,
    socket: Arc<UdpSocket>,
    env: Arc<Env>,
) {
    info!(
        "node_addr={}; Sending ACK {:?} items",
        node_name,
        &items.len()
    );

    let ack = GossipAck {
        items_received: items.to_vec(),
    };

    if let Ok(encoded_ack) = bincode::encode_to_vec(&ack, bincode::config::standard()) {
        match socket.send_to(&encoded_ack, &peer_addr).await {
            Ok(_) => {}
            Err(e) => {
                error!(
                    "node={}; Failed to send GossipAck to {}: {}",
                    node_name, peer_addr, e
                );
                return;
            }
        }
    } else {
        error!(
            "node={}; Failed to encode GossipAck; message not sent",
            node_name
        );
        return;
    }

    env.get_event_publisher()
        .write()
        .await
        .publish(&crate::event::Event {
            address_from: local_addr.clone(),
            address_to: peer_addr.clone(),
            message_type: "GossipAckSent".to_string(),
            data: serde_json::json!({
                "item_ids": items.iter().map(|x| x.storage_key.to_string()).collect::<Vec<_>>(),
            }),
            timestamp: now_millis(),
        })
        .await;

    info!(
        "node_addr={}; sent {:?} to {:?}",
        &local_addr, &ack, &peer_addr
    );
}

async fn handle_join_message(
    src: &SocketAddr,
    socket: Arc<UdpSocket>,
    sync_flag: Arc<Mutex<bool>>,
    message: GossipJoinMessage,
    state: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) {
    let mut node_state = state.write().await;
    let mut peers = HashMap::from([(message.node.address.clone(), message.node.clone())]);

    info!(
        "node={}; Join Message Received {:?} from {:?}:{:?}",
        node_state.get_address(),
        message,
        src.ip(),
        src.port()
    );

    match &mut *node_state {
        node::NodeState::Joined(node) => {
            if node.cluster_size() == node.cluster_config.cluster_size {
                error!("node={}; Cluster is full, cannot join", &node.address);

                env.get_event_publisher()
                    .write()
                    .await
                    .publish(&crate::event::Event {
                        address_from: message.node.address.clone(),
                        address_to: node.get_address().clone(),
                        message_type: "JoinRejected".to_string(),
                        data: serde_json::json!({
                            "reason": "Cluster is full",
                            "cluster_size": node.cluster_size(),
                            "max_size": node.cluster_config.cluster_size,
                        }),
                        timestamp: now_millis(),
                    })
                    .await;

                return;
            }

            peers.entry(src.to_string()).and_modify(|node| {
                node.last_seen = now_millis();
            });

            node.take_peers(&peers, true);

            // Respond to join with GossipMessage
            send_gossip_single(
                Some(&src.to_string()),
                socket.clone(),
                node,
                false,
                env.clone(),
            )
            .await;

            env.get_event_publisher()
                .write()
                .await
                .publish(&crate::event::Event {
                    address_from: node.get_address().clone(),
                    address_to: node.address.clone(),
                    message_type: "JoinAccepted".to_string(),
                    data: serde_json::json!({
                        "cluster_size": node.cluster_size(),
                        "joined_node": message.node.address.clone(),
                        "peers": node.all_peers.keys().cloned().collect::<Vec<_>>(),
                    }),
                    timestamp: now_millis(),
                })
                .await;
        }
        _ => {
            error!(
                "node={}; Cannot handle join message in current state",
                node_state.get_address()
            );
            return;
        }
    }
}

async fn handle_main_message(
    src: &SocketAddr,
    socket: Arc<UdpSocket>,
    sync_flag: Arc<Mutex<bool>>,
    message: GossipMessage,
    state: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) {
    let node_id = src.to_string();
    let mut node_state = state.write().await;
    info!(
        "node={}; Message Received {:?} from {:?}:{:?}",
        node_state.get_address(),
        message,
        src.ip(),
        src.port()
    );

    env.get_event_publisher().write().await.publish(&crate::event::Event {
        address_from: node_id.clone(),
        address_to: node_state.get_address().clone(),
        message_type: "GossipReceived".to_string(),
        data: serde_json::json!({
            "delta_size": message.items_delta.len(),
            "item_ids": message.items_delta.iter().map(|e| e.storage_key.to_string()).collect::<Vec<_>>(),
            "sync_request": message.sync_request.is_sync_request,
            "sync_response": message.sync_response,
        }),
        timestamp: now_millis(),
    }).await;

    match &mut *node_state {
        // if we are PreJoin and received a message, this means we got acknowledgement
        node::NodeState::PreJoin(node) => {
            node.node_hlc = HLC::merge(&node.node_hlc, &message.node_hlc, now_millis());

            let mut joined_node = match node
                .to_joined_state(
                    message.cluster_config.clone(),
                    message.partition_map.clone(),
                    env.get_store().read().await,
                )
                .await {
                    Ok(n) => n,
                    Err(e) => {
                        error!(
                            "node={}; Failed to transition to Joined state due to error: {}",
                            node.get_address(),
                            e.to_string()
                        );
                        return;
                    }
                };

            let address = joined_node.address.clone();
            let cluster_size = joined_node.cluster_config.cluster_size.clone();
            let all_peers = message.all_peers.clone();

            joined_node.take_peers(&message.all_peers, false);

            // Update partition counts
            joined_node.update_partition_counts(&node_id, message.partition_item_counts);

            // update node_state to joined state
            *node_state = NodeState::Joined(joined_node);
            // drop(node_state);

            env.get_event_publisher()
                .write()
                .await
                .publish(&crate::event::Event {
                    address_from: address.clone(),
                    address_to: address.clone(),
                    message_type: "Joined".to_string(),
                    data: serde_json::json!({
                        "cluster_size": cluster_size,
                        "peers": all_peers.keys().cloned().collect::<Vec<_>>(),
                    }),
                    timestamp: now_millis(),
                })
                .await;
        }
        node::NodeState::Joined(node) => {
            // Update peers to most recent timestamp and vector clock
            match message.node_hlc.compare(&node.node_hlc) {
                Ordering::Greater | Ordering::Equal => {
                    // TODO: ensure this is still valid, i.e. why would partition map change?
                    let membership_changed = node.all_peers != message.all_peers;
                    let partition_map_changed = node.partition_map != message.partition_map;

                    // Update partition counts
                    node.update_partition_counts(&node_id, message.partition_item_counts);

                    // Merge membership information
                    node.node_hlc = HLC::merge(&node.node_hlc, &message.node_hlc, now_millis());
                    if membership_changed || partition_map_changed {
                        node.take_peers(&message.all_peers, false);
                        info!(
                            "node={}; Received HLC is newer or equal to local HLC; Updated node_hlc: {:?}",
                            &node.address, node.node_hlc
                        );
                    }
                }
                Ordering::Less => {
                    if message.node_hlc.timestamp == 0 {
                        node.node_hlc.tick_hlc(now_millis());
                        if let Some(src_node) = message.all_peers.get(&src.to_string()) {
                            node.add_node(src_node.clone());
                            info!("node={}; Adding new node", &node.address);
                        } else {
                            error!(
                                "node={}; Cannot find source node in peers list",
                                &node.get_address()
                            );
                        }
                    }
                    info!(
                        "node={}; Received HLC is older to local HLC",
                        &node.get_address()
                    );
                }
            }

            // Update partition map information. This should happen only once during cluster initialization
            if node.partition_map.is_empty()
                || (node.cluster_size() as usize) > message.all_peers.len()
            {
                // Check if we need to update the partition map
                let mut partition_map = node.partition_map.clone();
                partition_map.assign(&node.all_peers.keys().cloned().collect::<Vec<_>>());
                node.node_hlc.tick_hlc(now_millis());

                node.partition_map = partition_map;
            }

            // Update received items and acknowledge
            // We must ensure ack is sent on every received item delta, not only new added items
            if message.items_delta.len() > 0 {
                let items = message.items_delta.clone();

                let store_guard = env.get_store().write().await;
                let added = node.add_items(&items, &src.to_string(), store_guard).await;

                if items.len() > 0 {
                    info!(
                        "node={}; Added new {} items from {}, sending {} Acks",
                        node.get_address(),
                        added.iter().count(),
                        &src,
                        items.len()
                    );
                    send_gossip_ack(
                        &node.get_address(),
                        &items,
                        &src.to_string(),
                        &node.get_address(),
                        socket.clone(),
                        env.clone(),
                    )
                    .await;

                    // Immediately forward newly received deltas to other peers so they don't
                    // have to wait for the periodic gossip interval. We rely on the
                    // items_delta_cache to avoid sending the delta back to the origin.
                    send_gossip_single(None, socket.clone(), node, false, env.clone()).await;
                } else {
                    debug!(
                        "node={}; No new items added from {}",
                        node.get_address(),
                        &src
                    );
                }
            }

            // Data sync
            // if message.sync_request.is_sync_request && node.cluster_size() > 1 {
            //     info!("node={}; Syncing back to {}", node.get_address(), &src);
            //     let mut items: Vec<ItemEntry> = vec![];

            //     if let Some(src_node) = node.all_peers.get(&src.to_string()) {
            //         let last_seen_hlc = HLC::new().tick_hlc(src_node.last_seen.clone());
            //         let store_guard = env.get_store().read().await;

            //         let items_since = match node.items_since(&last_seen_hlc, &store_guard).await {
            //             Ok(items) => items,
            //             Err(e) => {
            //                 error!("node={}; Failed to get items since {:?}: {}", node.get_address(), &last_seen_hlc, e.to_string());
            //                 vec![]
            //             }
            //         };

            //         for entry in items_since {
            //             let maybe_item = match node.get_item(&entry.item.id, &store_guard).await {
            //                 Ok(i) => i,
            //                 Err(e) => {
            //                     error!("node={}; Failed to get item {}: {}", node.get_address(), &entry.item.id, e.to_string());
            //                     continue;
            //                 }
            //             };

            //             if let Some(e) = maybe_item {
            //                 items.push(e.clone());
            //             }
            //         }
            //         info!(
            //             "node={}; Sync identifier {} items to send since {}",
            //             node.get_address(),
            //             &items.len(),
            //             src_node.last_seen
            //         );
            //     } else {
            //         error!(
            //             "node={}; Cannot find source node in peers list",
            //             &node.get_address()
            //         );
            //         return;
            //     }

            //     send_gossip_single(
            //         Some(&src.to_string()),
            //         socket.clone(),
            //         node,
            //         false,
            //         env.clone(),
            //     )
            //     .await;
            // }

            // bump last update time, it is important this happens after the sync
            node.all_peers.entry(src.to_string()).and_modify(|node| {
                node.last_seen = now_millis();
            });

            if message.sync_response {
                let mut sync = sync_flag.lock().await;
                *sync = false; // TODO: understand how this works
            }
        }
        _ => {
            error!(
                "node={}; Cannot handle gossip message in current state",
                node_state.get_address()
            );
            return;
        }
    }
}

async fn handle_item_delta_message(
    src: &SocketAddr,
    message: GossipAck,
    state: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) {
    let node_id = src.to_string();
    let mut node_state = state.write().await;
    info!(
        "node={}; Message GossipAck Received {:?} from {:?}:{:?}",
        node_state.get_address(),
        message,
        src.ip(),
        src.port()
    );

    match &mut *node_state {
        node::NodeState::Joined(node) => {
            env.get_event_publisher().write().await.publish(&crate::event::Event {
                address_from: src.to_string(),
                address_to: node.get_address().clone(),
                message_type: "GossipAckReceived".to_string(),
                data: serde_json::json!({
                    "item_ids": message.items_received.iter().map(|e| e.storage_key.to_string()).collect::<Vec<_>>(),
                }),
                timestamp: now_millis(),
            }).await;

            // Update partition counts
            node.update_partition_counts(&node_id, HashMap::new());

            // TODO: ensure error here is properly handled
            let _ = node.reconcile_delta_state(
                &src.to_string(),
                &message.items_received,
                &mut env.get_store().write().await,
            )
            .await;
        }
        _ => {
            error!(
                "node={}; Cannot handle item delta message in current state",
                src
            );
            return;
        }
    }
}

pub async fn send_gossip_single(
    peer_addr: Option<&String>,
    socket: Arc<UdpSocket>,
    node_state: &mut JoinedNode,
    is_sync: bool,
    env: Arc<Env>,
) {
    let mut peers: Vec<String> = vec![];
    if let Some(peer) = peer_addr {
        peers = vec![peer.clone()];
    } else if node_state.cluster_size() > 1 {
        peers = node_state.next_nodes(node_state.gossip_count());
    }

    // Send gossip to next nodes
    send_gossip_to_peers(&peers, node_state, false, is_sync, &socket, env.clone()).await;
}

pub async fn send_gossip_on_interval(
    local_addr: String,
    socket: Arc<UdpSocket>,
    node_state: Arc<RwLock<NodeState>>,
    sync_flag: Arc<Mutex<bool>>,
    env: Arc<Env>,
) {
    loop {
        let mut node_state = node_state.write().await;

        match &mut *node_state {
            node::NodeState::Joined(this_node) => {
                let sync_flag = sync_flag.lock().await;
                let now_millis = now_millis();

                // Remove peers that haven't responded within FAILURE_TIMEOUT
                let peers = this_node.all_peers.clone();
                for peer in peers.iter() {
                    if peer.0 != &local_addr
                        && now_millis - peer.1.last_seen > FAILURE_TIMEOUT.as_millis() as u64
                        && this_node.other_peers().len() > 0
                    {
                        this_node.remove_node(peer.0);
                        info!(
                            "node={}; Removed peer: {} from known peers",
                            &this_node.get_address(),
                            &peer.0
                        );
                    }
                }

                // Expire delta state if needed
                this_node.cleanup_expired_delta_state();

                // Send gossip to next nodes
                let gossip_count = this_node.gossip_count();
                let next_nodes = this_node.next_nodes(gossip_count);
                send_gossip_to_peers(
                    &next_nodes,
                    this_node,
                    sync_flag.clone(),
                    false,
                    &socket,
                    env.clone(),
                )
                .await;

                let node_address = this_node.get_address().clone();

                info!("node={}; Node State Joined", &node_address);
                info!("node={}; Node HLC", &node_address);
                info!(
                    "node={}; Known peers: {:?}",
                    &node_address,
                    &this_node.other_peers()
                );
                info!(
                    "node={}; Item count: {}",
                    &node_address,
                    &this_node.items_count()
                );
                info!(
                    "node={}; Delta Cache size: {}",
                    &node_address,
                    &this_node.items_delta_cache.iter().count()
                );
                info!(
                    "node={}; Delta State size: {}",
                    &node_address,
                    &this_node.items_delta_state.len()
                );
                info!(
                    "node={}; Partition: {:?}",
                    &node_address,
                    &this_node.partition_map.get_partitions_for_node(&local_addr)
                );
                info!(
                    "node={}; PartitionMap: {:?}",
                    &node_address, &this_node.partition_map
                );
            }
            node::NodeState::PreJoin(this_node) => {
                // Send join message to the peer node
                send_join_message(
                    &this_node.peer_node,
                    this_node,
                    false,
                    false,
                    &socket,
                    env.clone(),
                )
                .await;

                info!("node={}; Node PreJoin", this_node.get_address());
                info!(
                    "node={}; Node HLC: {:?}",
                    this_node.get_address(),
                    &this_node.node_hlc
                );
            }
            _ => {
                error!(
                    "node={}; Cannot send gossip in current state:",
                    node_state.get_address()
                );
                return;
            }
        }
        drop(node_state);

        sleep(GOSSIP_INTERVAL).await;
    }
}

async fn send_join_message(
    join_node: &String,
    node_state: &PreJoinNode,
    sync_flag: bool,
    is_sync_response: bool,
    socket: &UdpSocket,
    env: Arc<Env>,
) {
    let msg = GossipJoinMessage {
        node: Node {
            address: node_state.address.clone(),
            web_port: node_state.web_port.clone(),
            last_seen: 0,
        },
        peer_address: join_node.clone(),
        node_hlc: node_state.node_hlc.clone(),
        sync_request: SyncRequest {
            is_sync_request: sync_flag,
            since: HLC::new(),
        },
        sync_response: is_sync_response,
    };

    if let Ok(encoded) = bincode::encode_to_vec(&msg, bincode::config::standard()) {
        match socket.send_to(&encoded, &join_node).await {
            Ok(_) => {}
            Err(e) => {
                error!(
                    "node={}; Failed to send JoinMessage to {}: {}",
                    node_state.get_address(),
                    join_node,
                    e
                );
                return;
            }
        }
    } else {
        error!(
            "node={}; Failed to encode JoinMessage; message not sent",
            node_state.get_address()
        );
        return;
    }

    env.get_event_publisher()
        .write()
        .await
        .publish(&crate::event::Event {
            address_from: node_state.address.clone(),
            address_to: join_node.clone(),
            message_type: "JoinSent".to_string(),
            data: serde_json::json!({
                "sync_request": sync_flag,
                "sync_response": is_sync_response,
            }),
            timestamp: now_millis(),
        })
        .await;
}

async fn send_gossip_to_peers(
    next_nodes: &[String],
    node_state: &mut JoinedNode,
    sync_flag: bool,
    is_sync_response: bool,
    socket: &UdpSocket,
    env: Arc<Env>,
) {
    let mut all_delta_count = 0;

    let node_hlc = node_state.node_hlc.clone();
    let mut peer_deltas = Vec::with_capacity(next_nodes.len());
    {
        let store_guard = env.get_store().read().await;
        for peer_dest in next_nodes.iter() {
            let items_delta = match node_state.get_delta_for_node(peer_dest, &store_guard).await {
                Ok(delta) => delta,
                Err(e) => {
                    error!(
                        "node={}; Error getting deltas for node {}",
                        node_state.get_address(),
                        e
                    );
                    vec![]
                }
            };

            all_delta_count += items_delta.len();
            peer_deltas.push((peer_dest.clone(), items_delta));
        }
    }

    // send specific deltas to each peer
    for (peer_dest, items_delta) in peer_deltas.into_iter() {
        info!(
            "node={}; selected node: {}; sending delta size: {}",
            node_state.get_address(),
            &peer_dest,
            &items_delta.len()
        );

        let store_guard = env.get_store().read().await;

        let partition_counts = store_guard.partition_counts().await.unwrap_or(HashMap::new());

        let msg = GossipMessage {
            cluster_config: node_state.cluster_config.clone(),
            partition_map: node_state.partition_map.clone(),
            partition_item_counts: partition_counts,
            all_peers: node_state.all_peers.clone(),
            node_hlc: node_hlc.clone(),
            items_delta: items_delta.clone(),
            sync_request: SyncRequest {
                is_sync_request: sync_flag,
                since: HLC::new(),
            },
            sync_response: is_sync_response,
        };
        drop(store_guard);

        if let Ok(encoded) = bincode::encode_to_vec(&msg, bincode::config::standard()) {
            match socket.send_to(&encoded, &peer_dest).await {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        "node={}; Failed to send GossipMessage to {}: {}",
                        node_state.get_address(),
                        peer_dest,
                        e
                    );
                    return;
                }
            }
        } else {
            error!(
                "node={}; Failed to encode GossipMessage; message not sent",
                node_state.get_address()
            );
            return;
        }

        if !sync_flag {
            node_state.add_delta_state(
                &items_delta,
                DeltaAckState {
                    peers_pending: vec![peer_dest.clone()].into_iter().collect(),
                    created_at: now_millis(),
                },
            );

            // Remove delta items if everyone we know has received them
            if all_delta_count == 0 && !next_nodes.is_empty() {
                // TODO: ensure error here is properly handled
                let _ = node_state
                    .clear_delta(&mut env.get_store().write().await)
                    .await;
            }
        }

        env.get_event_publisher()
            .write()
            .await
            .publish(&crate::event::Event {
                address_from: node_state.get_address().clone(),
                address_to: peer_dest.clone(),
                message_type: "GossipSent".to_string(),
                data: serde_json::json!({
                    "delta_size": items_delta.len(),
                    "item_ids": items_delta.iter().map(|e| e.storage_key.to_string()).collect::<Vec<_>>(),
                    "sync_request": sync_flag,
                    "sync_response": is_sync_response,
                }),
                timestamp: now_millis(),
            })
            .await;

        info!(
            "node={}; sent {:?} to {:?}; sync={}",
            node_state.get_address(),
            msg,
            &peer_dest,
            &sync_flag
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
        let hlc1 = HLC {
            timestamp: 100,
            counter: 0,
        };
        let hlc2 = HLC {
            timestamp: 200,
            counter: 0,
        };
        let hlc3 = HLC {
            timestamp: 100,
            counter: 1,
        };

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
