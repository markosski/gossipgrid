use crate::gossip::{HLC, send_gossip_single};
use crate::node::NodeMemory;
use crate::now_millis;
use crate::item::{Item, ItemEntry, ItemStatus, ItemSubmit, ItemSubmitResponse};
use std::net::SocketAddr;
use bincode::de;
use log::info;
use serde::Serialize;
use uuid::Uuid;
use warp::http::Response;
use warp::hyper::Body;

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

#[derive(Debug, Clone, Copy)]
pub enum ProxyMethod {
    Get,
    Post,
    Put,
    Delete,
}

#[derive(Debug)]
struct MyError;
impl warp::reject::Reject for MyError {}

// TODO: handle errors where we attemp to send to another node
pub async fn proxy_request<T : Serialize>(
    host: &str,
    url: &str,
    method: ProxyMethod,
    body: Option<&T>
) -> Result<ItemSubmitResponse, String> {
    info!("Proxying request to {}/{}, {:?}, {:?}", host, url, &method, serde_json::to_string(&body));
    let client = reqwest::Client::new();

    let resp_builder = match method {
        ProxyMethod::Get => client.get(format!("http://{}/{}", host, url)),
        ProxyMethod::Post => client.post(format!("http://{}/{}", host, url))
            .json(&body.unwrap()),
        ProxyMethod::Put => client.put(format!("http://{}/{}", host, url)),
        ProxyMethod::Delete => client.delete(format!("http://{}/{}", host, url)),
    };

    let resp = resp_builder
        .send()
        .await.unwrap();

    // let warp_status: warp::http::StatusCode = resp.status().as_u16().try_into().unwrap();
    let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
    // let s = String::from_utf8(bytes.to_vec()).unwrap();
    let response_message = serde_json::from_slice::<ItemSubmitResponse>(&bytes)
        .map_err(|e| e.to_string())?;

    Ok(response_message)
}

// TODO: Implement the redirect filter for all endpoints
async fn handle_post_task(
    item: ItemSubmit,
    socket: Arc<UdpSocket>,
    memory: Arc<Mutex<NodeMemory>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let item_entry: ItemEntry;
    let item_id = match item.id {
        Some(id) => id,
        None => {
            info!("Item ID not provided, generating a new one");
            Uuid::new_v4().to_string()
        }
    };

    let mut memory = memory.lock().await;
    let response: ItemSubmitResponse;

    if memory.is_cluster_formed() {
        let routed_node = memory.partition_map.route(&item_id);
        info!("this node: {}, routed node: {:?}", &memory.this_node, &routed_node);
        if let Some(node) = routed_node {
            if memory.this_node != *node {
                let this_host: SocketAddr = node.parse().unwrap();
                let web_host = format!("{}:{}", this_host.ip(), 3002);
                info!("Routing item to remote node: {}", web_host);
                let new_item = ItemSubmit {
                    message: item.message.clone(),
                    id: Some(item_id.clone()),
                };
                response = proxy_request(&web_host, "items", ProxyMethod::Post, Some(&new_item)).await.unwrap();
            } else {
                info!("Routing to this node");
                let now = now_millis();

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
                memory.add_items(vec!(item_entry.clone()), &this_node).await;

                // TODO: send to all replicas
                send_gossip_single(
                    &this_node,
                    &vec![item_entry],
                    None,
                    socket.clone(),
                    &mut memory,
                    false,
                )
                .await;

                response = ItemSubmitResponse {
                    success: Some(vec![("id".to_string(), item_id)].into_iter().collect()),
                    error: None,
                };
            }
        } else {
            response = ItemSubmitResponse {
                success: None,
                error: Some(format!("Could not route to node, cluster not ready")),
            };
        }
    } else {
        response = ItemSubmitResponse {
            success: None,
            error: Some(format!("Cluster not formed")),
        };
    }
    return Ok(warp::reply::json(&response));
}

async fn handle_get_task(
    req_path: String,
    memory: Arc<Mutex<NodeMemory>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let item_id = req_path.split('/').last().unwrap_or_default().to_string();
    let memory = memory.lock().await;
    if let Some(item) = memory.get_item(&item_id).await {
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
    let tasks_count = memory.items_count().await;

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
    let is_success = memory.remove_item(&item_id).await;

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

    if memory.get_item(&item_id).await.is_some() {
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

        memory.add_items(vec!(new_item_entry.clone()), &this_node).await;
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
        .and(warp::put())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn simple_test() {
        let client = reqwest::Client::new();

        let payload = ItemSubmit {
            message: "Test item".to_string(),
            id: None
        };

        let req = client.post(format!("http://127.0.0.1:3002/items"))
            .json(&payload)
            .send()
            .await.unwrap();

        assert!(req.status().is_success());
    }
}