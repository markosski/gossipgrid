
use std::net::{UdpSocket, SocketAddr};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, UNIX_EPOCH, SystemTime};
use bincode::{Decode, Encode};
use rand::seq::IteratorRandom;
use log::{info, error};
use std::io;
use std::cmp::max;
use names::Generator;


const GOSSIP_INTERVAL: Duration = Duration::from_secs(5);
const FAILURE_TIMEOUT: Duration = Duration::from_secs(15);
const BUFFER_SIZE: usize = 1024;

#[derive(Debug, Clone, Encode, Decode)]
struct GossipMessage {
    known_peers: HashMap<String, u128>
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}

pub fn start_node() {
    // Generate name for this node
    let mut generator = Generator::default();
    let node_name = Arc::new(generator.next().unwrap());

    // Initiate peer information
    let default_host = "127.0.0.1:4109".to_string();
    let args: Vec<String> = std::env::args().collect();
    let local_addr = args.get(1).unwrap_or(&default_host).to_string();
    let seed_peer = args.get(2);

    info!(name=node_name.as_str(); "node={}; Starting application: {}", &node_name, &local_addr);

    let socket = UdpSocket::bind(local_addr.clone()).expect(&format!("Failed to bind socket {}", local_addr).to_string());
    socket.set_read_timeout(Some(Duration::from_secs(2))).expect("Failed to set read timeout");
    
    let peers: Arc<Mutex<HashMap<String, u128>>> = Arc::new(Mutex::new(HashMap::new())); // Stores peer addresses and last seen timestamp

    if let Some(peer) = seed_peer {
        peers.lock().unwrap().insert(peer.clone(), now_millis());
    }
    
    // Thread to receive gossip messages and update last seen times
    let socket_recv = socket.try_clone().expect("Failed to clone socket");
    let peers_recv = Arc::clone(&peers);
    let node_name_receiver = node_name.clone();

    thread::spawn(move || {
        let mut buf = [0u8; BUFFER_SIZE];
        loop {
            match socket_recv.recv_from(&mut buf) {
                Ok((size, src)) => {
                    let (received, _) = bincode::decode_from_slice::<GossipMessage, _>(&buf[..size], bincode::config::standard()).unwrap();
                    info!("node={}; received {:?} from {:?}:{:?}", node_name_receiver, received, src.ip(), src.port());
                    if let Ok(mut peers) = peers_recv.lock() {
                        // let mut peers = peers_recv.lock().unwrap();
                        let now_millis = now_millis();

                        // find peer with most revent timestamp
                        for (peer, timestamp) in received.known_peers {
                            let mut this_peer = peers.get(&peer);
                            let mut max_timestamp = timestamp;
                            if this_peer.is_some() {
                                max_timestamp = max(timestamp, this_peer.take().unwrap().clone());
                            }

                            if now_millis - max_timestamp < (FAILURE_TIMEOUT.as_millis()) as u128 {
                                peers.insert(peer, max_timestamp);
                            }
                        }
                        peers.insert(src.to_string(), now_millis); // Add sender with last seen timestamp
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
    let peers_send = Arc::clone(&peers);

    let node_name_sender = node_name.clone();
    let sender_handle = thread::spawn(move || {
        loop {
            // // lock will not be released in this case
            // let mut peers = match peers_send.lock() {
            //     Ok(peers) => peers,
            //     Err(poisoned) => {
            //         error!("Gossip thread panic detected! Lock was poisoned.");
            //         poisoned.into_inner()
            //     }
            // };

            // lock will be released after the if block
            if let Ok(mut peers) = peers_send.lock() {
                let now_millis = now_millis();
                peers.insert(local_addr.clone(), now_millis); // re-add current node
                
                // Remove peers that haven't responded within FAILURE_TIMEOUT
                peers.retain(|_, &mut last_seen| now_millis - last_seen < FAILURE_TIMEOUT.as_millis() as u128);

                // List of peers without this one
                let mut other_peers = peers.clone();
                other_peers.remove(&local_addr.clone());

                info!("node={}; Current state {:?}", node_name_sender, peers);
                
                if let Some(peer) = other_peers.keys().choose(&mut rand::rng()) {
                    let msg = GossipMessage { known_peers: peers.clone() };
                    let encoded = bincode::encode_to_vec(&msg, bincode::config::standard()).unwrap();
                    socket_send.send_to(&encoded, peer).ok();
                    info!("node={}; sent {:?} to {:?}", node_name_sender, msg, peer);
                }
            }
            thread::sleep(GOSSIP_INTERVAL);
        }
    });

    sender_handle.join().unwrap();
}