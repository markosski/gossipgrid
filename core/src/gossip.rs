use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH, SystemTime};
use bincode::{Decode, Encode};
use log::{info, error};
use crate::task::{Item, ItemStatus};
use std::cmp::Ordering;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use std::sync::Arc;
use tokio::time::sleep;

const GOSSIP_INTERVAL: Duration = Duration::from_secs(5);
const FAILURE_TIMEOUT: Duration = Duration::from_secs(15);
const BUFFER_SIZE: usize = 1024;

#[derive(Debug, Clone, Encode, Decode)]
pub struct HLC {
    pub timestamp: u64,
    pub counter: u64
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct TaskSet {
    pub tasks: Vec<Item>,
    pub hlc: HLC
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Node {
    pub address: String,
    pub last_seen: u64
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct GossipMessage {
    pub known_peers: HashMap<String, Node>,
    pub peers_hlc: HLC,
    pub tasks: HashMap<String, TaskSet>
}

pub struct NodeMemory {
    pub known_peers: HashMap<String, Node>,
    pub peers_hlc: HLC,
    pub tasks: HashMap<String, TaskSet>
}

impl NodeMemory {
    pub fn init(local_addr: String, seed_peer: Option<String>) -> NodeMemory {
        let mut known_peers = HashMap::new();
        known_peers.insert(local_addr.clone(), Node { address: local_addr.clone(), last_seen: now_millis()});
        if let Some(peer) = seed_peer {
            known_peers.insert(peer.clone(), Node { address: peer.clone(), last_seen: now_millis()});
        }
        NodeMemory {
            known_peers: known_peers,
            peers_hlc: HLC { timestamp: now_millis(), counter: 0 },
            tasks: [(local_addr.clone(), TaskSet { tasks: vec![], hlc: HLC { timestamp: now_millis(), counter: 0 } })].iter().cloned().collect()
        }
    }

    pub fn add_node(&mut self, node: Node) {
        self.known_peers.insert(node.address.clone(), node);
    }

    pub fn remove_node(&mut self, node_id: &String) {
        self.known_peers.remove(node_id);
        self.tasks.remove(node_id);
    }

    pub fn add_item(&mut self, node_id: String, item_id: String, message: String) {
        let item = Item { id: item_id, message: message, submitted_at: now_millis(), status: ItemStatus::Pending };

        let mut new_tasks = self.tasks.get_mut(&node_id).unwrap().tasks.clone();
        new_tasks.push(item);

        let new_task_set = TaskSet { 
            tasks: new_tasks, 
            hlc: self.tasks.get(&node_id).unwrap().hlc.tick_hlc(now_millis()) 
        };

        self.tasks.insert(node_id.clone(), new_task_set);
    }

    pub fn merge_tasks(&mut self, remote: &HashMap<String, TaskSet>) {
        for (node_id, remote_task) in remote.iter() {
            self.tasks
                .entry(node_id.clone())
                .and_modify(|local_task| {
                    // Merge based on HLC comparison
                    *local_task = match local_task.hlc.compare(&remote_task.hlc) {
                        std::cmp::Ordering::Less => remote_task.clone(),
                        _ => local_task.clone(), // Keep local if >= remote
                    };
                })
                .or_insert(remote_task.clone()); // Insert if missing
        }
    }

    pub fn pick_freshest_task_set(&self) -> Vec<Item> {
        self.tasks.clone()
        .values()
        .flat_map(|task_set| task_set.tasks.clone())
        .collect()
    }
}

impl HLC {
    fn compare(&self, other: &HLC) -> Ordering {
        match self.timestamp.cmp(&other.timestamp) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => self.counter.cmp(&other.counter),
        }
    }

    pub fn tick_hlc(&self, now: u64) -> HLC {
        if now > self.timestamp {
            HLC { timestamp: now, counter: 0 }
        } else {
            HLC { timestamp: self.timestamp, counter: self.timestamp + 1 }
        }
    }

    fn merge(local: &HLC, remote: &HLC, now: u64) -> HLC {
        let merged_pt = local.timestamp.max(remote.timestamp).max(now);

        let merged_lc = 
        if merged_pt == local.timestamp && merged_pt == remote.timestamp {
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

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}


pub async fn receive_gossip(node_name: String, socket: Arc<UdpSocket>, memory: Arc<Mutex<NodeMemory>>) {
    let mut buf = [0u8; BUFFER_SIZE];

    loop {
        match socket.recv_from(&mut buf).await {
            Ok((size, src)) => {
                let (received, _) = bincode::decode_from_slice::<GossipMessage, _>(&buf[..size], bincode::config::standard()).unwrap();
                info!("node={}; received {:?} from {:?}:{:?}", node_name, received, src.ip(), src.port());
                {    
                    let mut memory = memory.lock().await;

                    let maybe_this_peer = memory.known_peers.get(&src.to_string());

                    if let Some(_) = maybe_this_peer {
                        memory.add_node(Node { address: src.to_string(), last_seen: now_millis()});
                    } else {
                        memory.add_node(Node { address: src.to_string(), last_seen: now_millis()});
                    }

                    // Update peers to most recent timestamp and vector clock
                    let received_node_keys: Vec<&String> = received.known_peers.keys().collect();
                    let local_node_keys: Vec<&String> = memory.known_peers.keys().collect();

                    if received_node_keys == local_node_keys {
                        match received.peers_hlc.compare(&memory.peers_hlc) {
                            Ordering::Greater => {
                                memory.peers_hlc = HLC::merge(&memory.peers_hlc, &received.peers_hlc, now_millis());
                            }
                            Ordering::Equal | Ordering::Less => {
                                info!("node={}; Received HLC is older or equal to local HLC", node_name);
                                // Either local is up-to-date or newer; reject the membership update
                                // but you may still want to merge tasks separately later!
                            }
                        }
                    }

                    // update tasks
                    memory.merge_tasks(&received.tasks);

                    // bump last update time
                    memory.known_peers.entry(src.to_string()).and_modify(|node| {
                        node.last_seen = now_millis();
                    });
                }
            }
            Err(e) => {
                error!("Receive error: {}", e);
            }
        }
    }
}

pub async fn send_gossip(node_name: String, local_addr: String, socket: Arc<UdpSocket>, memory: Arc<Mutex<NodeMemory>>) {
    loop {
        // lock will be released after the block ends
        {
            let mut memory = memory.lock().await;
            let now_millis = now_millis();
            
            // Remove peers that haven't responded within FAILURE_TIMEOUT
            for peer in memory.known_peers.clone().iter() {
                if peer.0 != &local_addr && now_millis - peer.1.last_seen > FAILURE_TIMEOUT.as_millis() as u64 {
                    memory.remove_node(peer.0);
                    info!("node={}; Removed peer: {:?} from known peers", node_name, peer.0);
                } else if peer.0 != &local_addr {
                    memory.add_node(Node { address: peer.0.clone(), last_seen: now_millis});
                }
            }

            // List of peers without this one
            let mut other_peers = memory.known_peers.clone();
            other_peers.remove(&local_addr);

            info!("node={}; Known peers: {:?}", node_name, memory.known_peers);
            
            
            let vec =  Vec::from_iter(other_peers.keys().into_iter());
            if vec.len() > 0 {
                let index = rand::random_range(0..vec.len());
                if let Some(peer) = vec.get(index) {
                    let msg = GossipMessage { known_peers: memory.known_peers.clone(), peers_hlc: memory.peers_hlc.clone(), tasks: memory.tasks.clone() };
                    let encoded = bincode::encode_to_vec(&msg, bincode::config::standard()).unwrap();
                    socket.send_to(&encoded, peer).await.unwrap();
                    info!("node={}; sent {:?} to {:?}", node_name, msg, peer);
                }
            }
        
        }
        sleep(GOSSIP_INTERVAL).await;
    }
}