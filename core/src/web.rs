use crate::gossip;
use crate::gossip::{HLC, NodeMemory};
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
        is_cluster_formed = memory.other_peers().len() > 0;
        item_id = Uuid::new_v4().to_string();
        let item = Item {
            id: item_id.to_string(),
            message: item.message.clone(),
            submitted_at: 0,
        };

        item_entry = ItemEntry {
            item: item,
            status: ItemStatus::Active,
            hlc: HLC {
                timestamp: 0,
                counter: 0,
            },
        };
        memory.add_item(item_entry.clone());
        local_addr = memory.this_node.clone();

        if is_cluster_formed {
            gossip::send_gossip_single(
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
        success: Some(format!("Task received with id: {}", item_id)),
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
            success: Some(format!("entry: {:?}", &item)),
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
    let tasks_count = memory.count();

    let response = ItemSubmitResponse {
        success: Some(format!("count: {}", tasks_count)),
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
            success: Some(format!("Task deleted: {}", item_id)),
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
        .map(|id| id)
        .and(with_memory(memory.clone()))
        .and_then(handle_get_task);

    let remove_item = warp::path!("items" / String)
        .and(warp::delete())
        .map(|id| id)
        .and(with_memory(memory.clone()))
        .and_then(handle_remove_task);

    let address = local_web_addr.parse::<SocketAddr>().unwrap();
    warp::serve(post_item.or(get_item).or(get_items_count).or(remove_item))
        .run(address)
        .await;
}
