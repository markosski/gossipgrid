use log::info;
use names::Generator;
use crate::task::{ Item, ItemSubmit, ItemStatus};
use crate::gossip::{send_gossip, receive_gossip, NodeMemory, TaskSet, now_millis};
use uuid::Uuid;
use std::net::SocketAddr;

use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use std::sync::Arc;
use warp::Filter;

pub async fn start_node(web_addr: String, local_addr: String, node_memory: Arc<Mutex<NodeMemory>>) {
    // Generate name for this node
    let mut generator = Generator::default();
    let node_name = generator.next().unwrap();

    info!(name=node_name.as_str(); "node={}; Starting application: {}", &node_name, &local_addr);

    // Initialize sockets
    let socket = Arc::new(UdpSocket::bind(&local_addr).await
        .expect("Failed to set read timeout"));
    
    let _ = tokio::spawn(send_gossip(node_name.clone(), local_addr.clone(), socket.clone(), node_memory.clone()));
    let _ = tokio::spawn(receive_gossip(node_name.clone(), socket.clone(), node_memory.clone()));

    // Initiate HTTP server
    info!(name=node_name.as_str(); "node={}; Starting Web Server: {}", &node_name, &web_addr);
    web_server(web_addr.clone(), local_addr.clone(), node_memory.clone()).await;
}

fn with_memory(memory: Arc<Mutex<NodeMemory>>) -> impl Filter<Extract = (Arc<Mutex<NodeMemory>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || memory.clone())
}
fn with_local_addr(local_addr: String) -> impl Filter<Extract = (String,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || local_addr.clone())
}

async fn handle_post_task(
    task: ItemSubmit,
    memory: Arc<Mutex<NodeMemory>>,
    local_addr: String,
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut memory = memory.lock().await;
    let task_id = Uuid::new_v4();
    let task = Item { id: task_id.to_string(), message: task.message, submitted_at: now_millis(), status: ItemStatus::Pending };

    let mut new_tasks = memory.tasks.get_mut(&local_addr).unwrap().tasks.clone();
    new_tasks.push(task);

    let new_task_set = TaskSet { tasks: new_tasks, hlc: memory.tasks.get(&local_addr).unwrap().hlc.tick_hlc(now_millis()) };
    memory.tasks.insert(local_addr.clone(), new_task_set);

    Ok(warp::reply::json(&"Task received"))
}

async fn handle_get_tasks(
    memory: Arc<Mutex<NodeMemory>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let memory = memory.lock().await;
    let tasks = memory.pick_freshest_task_set();
    Ok(warp::reply::json(&tasks))
}

async fn web_server(local_web_addr: String, local_addr: String, memory: Arc<Mutex<NodeMemory>>) {
    let post_task = warp::path("tasks")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_memory(memory.clone()))
        .and(with_local_addr(local_addr.clone()))
        .and_then(handle_post_task);

    let get_tasks = warp::path("tasks")
        .and(warp::get())
        .and(with_memory(memory.clone()))
        .and_then(handle_get_tasks);

    let address = local_web_addr.parse::<SocketAddr>().unwrap();
    warp::serve(post_task.or(get_tasks)).run(address).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::gossip::HLC;

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