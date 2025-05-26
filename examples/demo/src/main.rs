use core::node::{ClusterConfig, NodeMemory};
use core::node;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    env_logger::init();

    // Initiate peer information
    let local_ip = "127.0.0.1";
    let default_port = "4109";
    let args: Vec<String> = std::env::args().collect();
    let web_addr = format!("{}:{}", &local_ip, args.get(1).unwrap());
    let local_addr = format!(
        "{}:{}",
        &local_ip,
        args.get(2).unwrap_or(&default_port.to_string())
    );
    let seed_peer = args.get(3).map(|s| s.to_string());

    let cluster_config = ClusterConfig {
        cluster_size: 3,
        partition_count: 8,
        replication_factor: 2,
    };

    // Initialize node memory
    let web_port: u16 = args.get(1).unwrap().parse().expect("Invalid port number");
    let node_memory = Arc::new(Mutex::new(NodeMemory::init(
        local_addr.clone(),
        web_port,
        seed_peer,
        cluster_config,
    )));

    node::start_node(web_addr, local_addr, node_memory.clone()).await;
}
