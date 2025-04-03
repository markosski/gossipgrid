use log::info;
use names::Generator;
use crate::item::{ItemSubmit, ItemSubmitResponse};
use crate::gossip::{send_gossip, receive_gossip, NodeMemory};
use uuid::Uuid;
use std::net::SocketAddr;

use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use std::sync::Arc;
use warp::Filter;

// Main entry point for the node
pub async fn start_node(web_addr: String, local_addr: String, node_memory: Arc<Mutex<NodeMemory>>) {
    // Generate name for this node
    let mut generator = Generator::default();
    let node_name = generator.next().unwrap();

    info!(name=node_name.as_str(); "node={}; Starting application: {}", &node_name, &local_addr);

    // Initialize socket
    let socket = Arc::new(UdpSocket::bind(&local_addr).await
        .expect("Failed to set read timeout"));
    
    // Fire and forget gossip tasks
    let _ = tokio::spawn(send_gossip(node_name.clone(), local_addr.clone(), socket.clone(), node_memory.clone()));
    let _ = tokio::spawn(receive_gossip(node_name.clone(), socket.clone(), node_memory.clone()));

    // Initiate HTTP server
    info!(name=node_name.as_str(); "node={}; Starting Web Server: {}", &node_name, &web_addr);
    web_server(web_addr.clone(), local_addr.clone(), node_memory.clone()).await;
}

// HTTP server implementation
fn with_memory(memory: Arc<Mutex<NodeMemory>>) -> impl Filter<Extract = (Arc<Mutex<NodeMemory>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || memory.clone())
}
fn with_local_addr(local_addr: String) -> impl Filter<Extract = (String,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || local_addr.clone())
}

async fn handle_post_task(
    item: ItemSubmit,
    memory: Arc<Mutex<NodeMemory>>,
    local_addr: String,
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut memory = memory.lock().await;
    let item_id = Uuid::new_v4();
    memory.add_item(&local_addr, item_id.to_string(), item.message);

    let response = ItemSubmitResponse { 
        success: Some(format!("Task received with id: {}", item_id)), 
        error: None
    };

    Ok(warp::reply::json(&response))
}

async fn handle_get_tasks(
    memory: Arc<Mutex<NodeMemory>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let memory = memory.lock().await;
    let tasks = memory.pick_freshest_task_set();

    Ok(warp::reply::json(&tasks))
}

async fn handle_remove_task(
    req_path: String,
    memory: Arc<Mutex<NodeMemory>>,
    local_addr: String,
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut memory = memory.lock().await;
    let item_id = req_path.split('/').last().unwrap_or_default().to_string();
    let is_success = memory.remove_item(&local_addr, &item_id);

    let response: ItemSubmitResponse;
    if is_success {
        response = ItemSubmitResponse { 
            success: Some(format!("Task deleted: {}", item_id)), 
            error: None
        };
    } else {
        response = ItemSubmitResponse { 
            success: None, 
            error: Some(format!("Task not found: {}", item_id)) 
        };
    }
    
    Ok(warp::reply::json(&response))
}

async fn web_server(local_web_addr: String, local_addr: String, memory: Arc<Mutex<NodeMemory>>) {
    let post_task = warp::path("items")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_memory(memory.clone()))
        .and(with_local_addr(local_addr.clone()))
        .and_then(handle_post_task);

    let get_tasks = warp::path("items")
        .and(warp::get())
        .and(with_memory(memory.clone()))
        .and_then(handle_get_tasks);

    let remove_task = warp::any()
        .and(warp::delete())
        .and(warp::path::full()).map(|path: warp::path::FullPath| {
            path.as_str().to_string()
        })
        .and(with_memory(memory.clone()))
        .and(with_local_addr(local_addr.clone()))
        .and_then(handle_remove_task);

    let address = local_web_addr.parse::<SocketAddr>().unwrap();
    warp::serve(post_task.or(get_tasks).or(remove_task)).run(address).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::gossip::{HLC, TaskSet};
    use crate::item::{Item, ItemStatus};

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
                    tasks: vec![Item {
                        id: "task1".to_string(),
                        message: "task1 message".to_string(),
                        submitted_at: 100,
                        status: ItemStatus::Pending,
                    }],
                    hlc: HLC { timestamp: 101, counter: 0 },
                },
            );
            tasks
        };

        local_memory.tasks.insert(
            "node1".to_string(),
            TaskSet {
                tasks: vec![Item {
                    id: "task2".to_string(),
                    message: "task2 message".to_string(),
                    submitted_at: 100,
                    status: ItemStatus::Pending,
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