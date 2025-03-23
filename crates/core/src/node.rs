
use std::net::{UdpSocket};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, UNIX_EPOCH, SystemTime};
use bincode::{Decode, Encode};
use rand::seq::IteratorRandom;
use log::{info, error};
use std::io;
use names::Generator;
use crate::task::{ Task, TaskStatus};
use tiny_http::{Server, Response, Request};
use std::cmp::Ordering;
use uuid::Uuid;


const GOSSIP_INTERVAL: Duration = Duration::from_secs(5);
const FAILURE_TIMEOUT: Duration = Duration::from_secs(15);
const BUFFER_SIZE: usize = 1024;

#[derive(Debug, Clone, Encode, Decode)]
struct HLC {
    timestamp: u64,
    counter: u64
}

#[derive(Debug, Clone, Encode, Decode)]
struct TaskSet {
    tasks: Vec<Task>,
    hlc: HLC
}

#[derive(Debug, Clone, Encode, Decode)]
struct Node {
    address: String,
    last_seen: u64
}

#[derive(Debug, Clone, Encode, Decode)]
struct GossipMessage {
    known_peers: HashMap<String, Node>,
    peers_hlc: HLC,
    tasks: HashMap<String, TaskSet>
}

struct NodeMemory {
    known_peers: HashMap<String, Node>,
    peers_hlc: HLC,
    tasks: HashMap<String, TaskSet>
}

impl NodeMemory {
    fn merge_tasks(&mut self, remote: &HashMap<String, TaskSet>) {
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

    fn pick_freshest_task_set(&self) -> Vec<Task> {
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

    fn tick_hlc(&self, now: u64) -> HLC {
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

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

pub fn start_node() {
    // Generate name for this node
    let mut generator = Generator::default();
    let node_name = Arc::new(generator.next().unwrap());

    // Initiate peer information
    let default_host = "127.0.0.1:4109".to_string();
    let args: Vec<String> = std::env::args().collect();
    let web_port = args.get(1).unwrap();
    let local_addr = args.get(2).unwrap_or(&default_host).to_string();
    let seed_peer = args.get(3);

    // Initiate HTTP server
    let web_server_host = format!("0.0.0.0:{}", web_port);
    let server = Server::http(&web_server_host).unwrap();
    println!("Listening on http://{:?}", &web_server_host);

    info!(name=node_name.as_str(); "node={}; Starting application: {}", &node_name, &local_addr);

    // Initialize node memory
    let mut local_node_memory = NodeMemory {
        known_peers: HashMap::new(), 
        peers_hlc: HLC {timestamp: now_millis(), counter: 0},
        tasks: [(local_addr.clone(), TaskSet {tasks: vec!(), hlc: HLC {timestamp: now_millis(), counter: 0}})].iter().cloned().collect()
    };

    local_node_memory.known_peers.insert(local_addr.clone(), Node { address: local_addr.clone(), last_seen: now_millis()});
    if let Some(peer) = seed_peer {
        local_node_memory.known_peers.insert(peer.clone(), Node { address: peer.clone(), last_seen: now_millis()});
    }
    let node_memory: Arc<Mutex<NodeMemory>> = Arc::new(Mutex::new(local_node_memory)); // Stores peer addresses and last seen timestamp

    // Initialize sockets
    let socket = UdpSocket::bind(&local_addr).expect(&format!("Failed to bind socket {}", &local_addr).to_string());
    socket.set_read_timeout(Some(Duration::from_secs(2))).expect("Failed to set read timeout");
    
    // Thread to receive gossip messages and update last seen times
    let socket_recv = socket.try_clone().expect("Failed to clone socket");
    let memory_recv = node_memory.clone();
    let node_name_receiver = node_name.clone();

    thread::spawn(move || {
        let mut buf = [0u8; BUFFER_SIZE];
        loop {
            match socket_recv.recv_from(&mut buf) {
                Ok((size, src)) => {
                    let (received, _) = bincode::decode_from_slice::<GossipMessage, _>(&buf[..size], bincode::config::standard()).unwrap();
                    info!("node={}; received {:?} from {:?}:{:?}", node_name_receiver, received, src.ip(), src.port());
                    if let Ok(mut memory) = memory_recv.lock() {

                        let maybe_this_peer = memory.known_peers.get(&src.to_string());

                        if let Some(_) = maybe_this_peer {
                            memory.known_peers.insert(src.to_string(), Node { address: src.to_string(), last_seen: now_millis()});
                        } else {
                            memory.known_peers.insert(src.to_string(), Node { address: src.to_string(), last_seen: now_millis()});
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
                                    info!("node={}; Received HLC is older or equal to local HLC", node_name_receiver);
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
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(100)); // Prevent busy waiting
                }
                Err(e) => {
                    error!("Receive error: {}", e);
                }
            }
        }
    });

    // Thread to send gossip messages
    let socket_send = socket.try_clone().expect("Failed to clone socket");
    let memory_send = node_memory.clone();

    let node_name_sender = node_name.clone();
    let local_addr_sender = local_addr.clone();
    let sender_handle = thread::spawn(move || {
        loop {
            // lock will be released after the if block
            if let Ok(mut memory) = memory_send.lock() {
                let now_millis = now_millis();
                
                // Remove peers that haven't responded within FAILURE_TIMEOUT
                let mut peers = memory.known_peers.clone();
                for peer in memory.known_peers.iter() {
                    if peer.0 != &local_addr_sender && now_millis - peer.1.last_seen > FAILURE_TIMEOUT.as_millis() as u64 {
                        peers.remove(peer.0);
                        info!("node={}; Removed peer: {:?} from known peers", node_name_sender, peer.0);
                    } else if peer.0 != &local_addr_sender{
                        // let this_peer = memory.known_peers.get(&local_addr_sender).unwrap();
                        peers.insert(local_addr_sender.clone(), Node { address: peer.0.clone(), last_seen: now_millis});
                    }
                }
                memory.known_peers = peers;

                // List of peers without this one
                let mut other_peers = memory.known_peers.clone();
                other_peers.remove(&local_addr_sender);

                info!("node={}; Known peers: {:?}", node_name_sender, memory.known_peers);
                
                if let Some(peer) = other_peers.keys().choose(&mut rand::rng()) {
                    let msg = GossipMessage { known_peers: memory.known_peers.clone(), peers_hlc: memory.peers_hlc.clone(), tasks: memory.tasks.clone() };
                    let encoded = bincode::encode_to_vec(&msg, bincode::config::standard()).unwrap();
                    socket_send.send_to(&encoded, peer).ok();
                    info!("node={}; sent {:?} to {:?}", node_name_sender, msg, peer);
                }
            }
            thread::sleep(GOSSIP_INTERVAL);
        }
    });

    fn handle_request(mut req: Request, local_addr: String, request_memory: Arc<Mutex<NodeMemory>>) {
        if req.url() == "/task" && req.method() == &tiny_http::Method::Post {
            let mut content = String::new();
            let read_bytes = req.as_reader().read_to_string(&mut content).unwrap();
            println!("Received task with size {} and message: {}", read_bytes, content);

            if let Ok(mut memory) = request_memory.lock() {
                let task_id = Uuid::new_v4();
                let task = Task {id: task_id.to_string(), message: content, submitted_at: now_millis(), status: TaskStatus::Pending};

                let mut new_tasks = memory.tasks.get_mut(&local_addr).unwrap().tasks.clone();
                new_tasks.push(task);

                let new_task_set = TaskSet {tasks: new_tasks, hlc: memory.tasks.get(&local_addr).unwrap().hlc.tick_hlc(now_millis())};
                memory.tasks.insert(local_addr.clone(), new_task_set);
            }

            // Add to your internal queue here
            let response = Response::from_string("Task received");
            req.respond(response).unwrap();

        } else if req.url() == "/task" && req.method() == &tiny_http::Method::Patch {
            // update existing task?

        } else if req.url() == "/task" && req.method() == &tiny_http::Method::Get {
            if let Ok(memory) = request_memory.lock() {
                let tasks = memory.pick_freshest_task_set().clone();
                let response = Response::from_string(format!("{:?}", tasks));
                req.respond(response).unwrap();
            }
        } else {
            req.respond(Response::from_string("Not Found")).unwrap();
        }
    }

    for req in server.incoming_requests() {
        // Spawn a thread for each request (optional, depends on how you want to handle concurrency)
        let node_memory_pt = node_memory.clone();
        let local_addr_arg = local_addr.clone();
        thread::spawn(move || handle_request(req, local_addr_arg, node_memory_pt));
    }

    sender_handle.join().unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_tasks() {
        let mut local_memory = NodeMemory {
            known_peers: HashMap::new(),
            peers_hlc: HLC { timestamp: 100, counter: 0 },
            tasks: HashMap::new(),
        };

        let remote_tasks = {
            let mut tasks = HashMap::new();
            tasks.insert(
                "node1".to_string(),
                TaskSet {
                    tasks: vec![Task {
                        id: "task1".to_string(),
                        message: "task1 message".to_string(),
                        submitted_at: 100,
                        status: TaskStatus::Pending,
                    }],
                    hlc: HLC { timestamp: 101, counter: 0 },
                },
            );
            tasks
        };

        local_memory.tasks.insert(
            "node1".to_string(),
            TaskSet {
                tasks: vec![Task {
                    id: "task2".to_string(),
                    message: "task2 message".to_string(),
                    submitted_at: 100,
                    status: TaskStatus::Pending,
                }],
                hlc: HLC { timestamp: 100, counter: 0 },
            },
        );

        local_memory.merge_tasks(&remote_tasks);

        let merged_task_set = local_memory.tasks.get("node1").unwrap();
        assert_eq!(merged_task_set.tasks.len(), 1);
        assert_eq!(merged_task_set.tasks[0].id, "task1");
        assert_eq!(merged_task_set.hlc.timestamp, 101);
    }
}