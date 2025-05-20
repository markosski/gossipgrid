use crate::gossip::{HLC, send_gossip_single};
use crate::node::NodeMemory;
use crate::now_millis;
use crate::item::{Item, ItemEntry, ItemStatus, ItemSubmit, ItemSubmitResponse};
use std::net::SocketAddr;
use log::info;
use uuid::Uuid;

use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use warp::Filter;

// HTTP server implementation
fn with_memory(
    memory: Arc<Mutex<NodeMemory>>,
) -> impl Filter<Extract = (Arc<Mutex<NodeMemory>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || memory.clone())
}

fn with_socket(
    socket: Arc<UdpSocket>,
) -> impl Filter<Extract = (Arc<UdpSocket>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || socket.clone())
}

async fn handle_post_task(
    item: ItemSubmit,
    socket: Arc<UdpSocket>,
    memory: Arc<Mutex<NodeMemory>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let item_entry: ItemEntry;
    let local_addr: String;
    let item_id: String;

    let is_cluster_formed: bool;
    {
        let mut memory = memory.lock().await;
        let now = now_millis();
        is_cluster_formed = memory.other_peers().len() > 0;
        item_id = Uuid::new_v4().to_string();

        let item = Item {
            id: item_id.to_string(),
            message: item.message.clone(),
            submitted_at: now.clone(),
        };

        item_entry = ItemEntry {
            item: item,
            status: ItemStatus::Active,
            hlc: HLC {
                timestamp: now.clone(),
                counter: 0,
            },
        };
        let this_node = memory.this_node.clone();
        memory.add_items(vec!(item_entry.clone()), &this_node);
        local_addr = memory.this_node.clone();

        if is_cluster_formed {
            send_gossip_single(
                &this_node,
                &vec![item_entry],
                None,
                &local_addr,
                socket.clone(),
                &mut memory,
                false,
            )
            .await;
        } else {
            info!("Cluster not formed, not sending gossip");
        }
    }

    let response = ItemSubmitResponse {
        success: Some(vec![("id".to_string(), item_id)].into_iter().collect()),
        error: None,
    };

    Ok(warp::reply::json(&response))
}

async fn handle_get_task(
    req_path: String,
    memory: Arc<Mutex<NodeMemory>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let item_id = req_path.split('/').last().unwrap_or_default().to_string();
    let memory = memory.lock().await;
    if let Some(item) = memory.items.get(&item_id) {
        let response = ItemSubmitResponse {
            success: Some(vec![("item".to_string(), format!("{:?}", item))].into_iter().collect()),
            error: None,
        };
        Ok(warp::reply::json(&response))
    } else {
        let response = ItemSubmitResponse {
            success: None,
            error: Some(format!("Task not found: {}", item_id)),
        };
        Ok(warp::reply::json(&response))
    }
}

async fn handle_get_tasks_count(
    memory: Arc<Mutex<NodeMemory>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let memory = memory.lock().await;
    let tasks_count = memory.items_count();

    let response = ItemSubmitResponse {
        success: Some(vec![("count".to_string(), tasks_count.to_string())].into_iter().collect()),
        error: None,
    };

    Ok(warp::reply::json(&response))
}

async fn handle_remove_task(
    req_path: String,
    memory: Arc<Mutex<NodeMemory>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let item_id = req_path.split('/').last().unwrap_or_default().to_string();
    let mut memory = memory.lock().await;
    let is_success = memory.remove_item(&item_id);

    let response: ItemSubmitResponse;
    if is_success {
        response = ItemSubmitResponse {
            success: Some(vec![("id".to_string(), item_id)].into_iter().collect()),
            error: None,
        };
    } else {
        response = ItemSubmitResponse {
            success: None,
            error: Some(format!("Task not found: {}", item_id)),
        };
    }

    Ok(warp::reply::json(&response))
}

async fn handle_update_task(
    item: ItemSubmit,
    req_path: String,
    memory: Arc<Mutex<NodeMemory>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let item_id = req_path.split('/').last().unwrap_or_default().to_string();
    let mut memory = memory.lock().await;
    let this_node = memory.this_node.clone();
    let response: ItemSubmitResponse;

    if memory.items.contains_key(&item_id) {
        let new_item_entry = ItemEntry {
            item: Item {
                id: item_id.to_string(),
                message: item.message.clone(),
                submitted_at: now_millis(),
            },
            status: ItemStatus::Active,
            hlc: HLC {
                timestamp: now_millis(),
                counter: 0,
            },
        };

        memory.add_items(vec!(new_item_entry.clone()), &this_node);
        response = ItemSubmitResponse {
            success: Some(vec![("message".to_string(), "Task updated".to_string()), ("id".to_string(), item_id.clone())].into_iter().collect()),
            error: None,
        };
    } else {
        response = ItemSubmitResponse {
            success: None,
            error: Some(format!("Item not found: {}", item_id)),
        };
    }

    Ok(warp::reply::json(&response))
}

pub async fn web_server(
    local_web_addr: String,
    socket: Arc<UdpSocket>,
    memory: Arc<Mutex<NodeMemory>>,
) {
    let post_item = warp::path("items")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_socket(socket.clone()))
        .and(with_memory(memory.clone()))
        .and_then(handle_post_task);

    let get_items_count = warp::path("items")
        .and(warp::get())
        .and(with_memory(memory.clone()))
        .and_then(handle_get_tasks_count);

    let get_item = warp::path!("items" / String)
        .and(warp::get())
        .and(with_memory(memory.clone()))
        .and_then(handle_get_task);

    let remove_item = warp::path!("items" / String)
        .and(warp::delete())
        .and(with_memory(memory.clone()))
        .and_then(handle_remove_task);

    let update_item = warp::path!("items" / String)
        .and(warp::patch())
        .and(with_memory(memory.clone()))
        .and(warp::body::json())
        .and_then(|req_path: String, memory: Arc<Mutex<NodeMemory>>, item: ItemSubmit| {
            handle_update_task(item, req_path, memory)
        });

    let address = local_web_addr.parse::<SocketAddr>().unwrap();
    warp::serve(post_item.or(get_item).or(get_items_count).or(remove_item).or(update_item))
        .run(address)
        .await;
}
