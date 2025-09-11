use gossipgrid::{env::{self, Env}, node::{self, NodeState}};
use gossipgrid::{event::EventPublisherFileLogger, store::memory_store::InMemoryStore};
use std::sync::Arc;

use tokio::sync::RwLock;

#[cfg(test)]
mod tests {
    use gossipgrid::node::ClusterConfig;
    use log::info;

    use super::*;

    #[tokio::test]
    async fn test_start_cluster() {
        env_logger::init();

        let local_addr = "127.0.0.1:4009".to_string();
        let local_addr_2 = "127.0.0.1:4010".to_string();
        let web_port = 3001;
        let web_port_2 = 3002;

        let env : Arc<Env> = Arc::new(
            env::Env::new(
                Box::new(InMemoryStore::new()),
                Box::new(EventPublisherFileLogger {
                    file_path: "events.log".to_string(),
                }),
            )
        );

        let cluster_config = ClusterConfig {
                cluster_size: 2,
                partition_count: 4,
                replication_factor: 2,
            };
        
        let node_memory_1 = Arc::new(RwLock::new(NodeState::init(
            local_addr.clone(),
            web_port,
            None,
            Some(cluster_config),
        )));

        let node_memory_2 = Arc::new(RwLock::new(NodeState::init(
            local_addr_2.clone(),
            web_port_2,
            Some(local_addr.clone()),
            None,
        )));
        
        let node_1 = tokio::spawn(node::start_node("127.0.0.1:3001".to_string(), local_addr.clone(), node_memory_1.clone(), env.clone()));
        let node_2 = tokio::spawn(node::start_node("127.0.0.1:3002".to_string(), local_addr_2.clone(), node_memory_2.clone(), env.clone()));

        let mut counter = 0;
        loop {
            if counter == 5 {
                panic!("Nodes did not join the cluster in time");
            }

            let node_mem_1 = node_memory_1.read().await;
            let node_mem_2 = node_memory_2.read().await;

            let joined_1 = match &*node_mem_1 {
                NodeState::Joined(_) => { 
                    info!("Node 1 has joined the cluster");
                    true
                },
                _ => {
                    false
                }
            };

            let joined_2 = match &*node_mem_2 {
                NodeState::Joined(_) => { 
                    info!("Node 2 has joined the cluster");
                    true
                },
                _ => {
                    false
                }
            };

            if joined_1 && joined_2 {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            // increment counter
            counter += 1;
        }

        node_1.abort();
        node_2.abort();
    }
}