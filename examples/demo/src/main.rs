pub use gossipgrid::node::{ClusterConfig, NodeState};
use gossipgrid::{env, node};
use gossipgrid::{env::Env, event::EventPublisherFileLogger};
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    // Initiate peer information
    const LOCAL_IP_DEFAULT: &str = "127.0.0.1";
    const CLUSTER_PARTITION_COUNT: u16 = 8;
    const CLUSTER_REPLICATION_FACTOR: u8 = 2;

    env_logger::init();

    let cli_matched = gossipgrid::cli::node_cli().get_matches();

    match cli_matched.subcommand() {
        Some(("cluster", sub_matches)) => {
            let size: u8 = sub_matches
                .get_one::<String>("size")
                .unwrap()
                .parse()
                .expect("Invalid size");
            let web_port: u16 = sub_matches
                .get_one::<String>("web-port")
                .unwrap()
                .parse()
                .expect("Invalid web port");
            let node_port: u16 = sub_matches
                .get_one::<String>("node-port")
                .unwrap()
                .parse()
                .expect("Invalid node port");
            let partition_size: u16 = sub_matches
                .get_one::<String>("partition-size")
                .unwrap_or(&CLUSTER_PARTITION_COUNT.to_string())
                .parse()
                .expect("Invalid partition size");
            let replication_factor: u8 = sub_matches
                .get_one::<String>("replication-factor")
                .unwrap_or(&CLUSTER_REPLICATION_FACTOR.to_string())
                .parse()
                .expect("Invalid replication factor");

            if replication_factor > size {
                eprintln!("Error: replication-factor must be less than cluster size.");
                std::process::exit(1);
            }

            let local_addr = format!("{}:{}", &LOCAL_IP_DEFAULT, &node_port.to_string());

            let web_addr = format!("{}:{}", &LOCAL_IP_DEFAULT, &web_port.to_string());

            let cluster_config = ClusterConfig {
                cluster_size: size,
                partition_count: partition_size,
                replication_factor: replication_factor,
            };
            let node_memory = Arc::new(RwLock::new(NodeState::init(
                local_addr.clone(),
                web_port,
                None,
                Some(cluster_config),
            )));

            let env: Arc<Env> = Arc::new(env::Env::new(
                Box::new(gossipgrid::store::memory_store::InMemoryStore::new()),
                Box::new(
                    EventPublisherFileLogger::new(format!("events_{}.log", &local_addr)).await,
                ),
            ));

            node::start_node(web_addr, local_addr, node_memory, env).await
        }
        Some(("join", sub_matches)) => {
            let peer_address = sub_matches
                .get_one::<String>("address")
                .expect("Address is required");
            let web_port: u16 = sub_matches
                .get_one::<String>("web-port")
                .unwrap()
                .parse()
                .expect("Invalid web port");
            let node_port: u16 = sub_matches
                .get_one::<String>("node-port")
                .unwrap()
                .parse()
                .expect("Invalid node port");

            let local_addr = format!("{}:{}", &LOCAL_IP_DEFAULT, &node_port.to_string());

            let web_addr = format!("{}:{}", &LOCAL_IP_DEFAULT, &web_port.to_string());

            let node_memory = Arc::new(RwLock::new(NodeState::init(
                local_addr.clone(),
                web_port,
                Some(peer_address.clone()),
                None,
            )));

            let env: Arc<Env> = Arc::new(env::Env::new(
                Box::new(gossipgrid::store::memory_store::InMemoryStore::new()),
                Box::new(
                    EventPublisherFileLogger::new(format!("events_{}.log", &local_addr)).await,
                ),
            ));

            node::start_node(web_addr, local_addr, node_memory, env).await
        }
        _ => unreachable!(),
    }
    .expect("Node failed");
}
