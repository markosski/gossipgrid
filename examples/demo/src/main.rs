use core::node;
use core::gossip::NodeMemory;
use tokio::sync::Mutex;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    env_logger::init();


    // Initiate peer information
    let local_ip = "127.0.0.1";
    let default_port = "4109";
    let args: Vec<String> = std::env::args().collect();
    let web_addr = format!("{}:{}", &local_ip, args.get(1).unwrap());
    let local_addr = format!("{}:{}", &local_ip, args.get(2).unwrap_or(&default_port.to_string()));
    let seed_peer = args.get(3).map(|s| s.to_string());

    // Initialize node memory
    let node_memory = 
        Arc::new(
            Mutex::new(
                NodeMemory::init(local_addr.clone(), seed_peer)));

    node::start_node(web_addr, local_addr, node_memory.clone()).await;
}