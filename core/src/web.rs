use crate::env::Env;
use crate::gossip::{HLC, send_gossip_single};
use crate::node::{self, NodeState};
use crate::{now_millis};
use crate::item::{Item, ItemEntry, ItemStatus, ItemSubmit, ItemSubmitResponse, ItemUpdate};
use std::net::SocketAddr;
use log::{error,info};
use serde::Serialize;

use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::{RwLock};
use warp::Filter;

// HTTP server implementation
fn with_memory(
    memory: Arc<RwLock<NodeState>>,
) -> impl Filter<Extract = (Arc<RwLock<NodeState>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || memory.clone())
}

fn with_socket(
    socket: Arc<UdpSocket>,
) -> impl Filter<Extract = (Arc<UdpSocket>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || socket.clone())
}

fn with_env(
    env: Arc<Env>,
) -> impl Filter<Extract = (Arc<Env>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || env.clone())
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
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut memory = memory.write().await;
    match &mut *memory {
        node::NodeState::Joined(node) => {
            let item_entry: ItemEntry;
            let response: ItemSubmitResponse;

            if !node.is_cluster_formed() {
                response = ItemSubmitResponse {
                    success: None,
                    error: Some(format!("Cluster not formed")),
                };
                return Ok(warp::reply::json(&response));
            }

            let routed_node = node.partition_map.route(&node.address, &item.id);
            info!("this node: {}, routed node: {:?}", &node.address, &routed_node);
            if let Some(node_address) = routed_node {
                if node.address != *node_address {
                    let this_host: SocketAddr = node_address.parse().unwrap();
                    let this_host_ip = this_host.ip().to_string();
                    let mut nodes = node.other_peers().clone();
                    nodes.retain(|k, _| *k == &node_address);

                    if nodes.is_empty() {
                        error!("Node for router address not found: {}, known nodes: {:?}", &this_host_ip, &nodes);
                    }
                    let router_web_port = nodes.iter().last().unwrap().1.web_port;

                    let web_host = format!("{}:{}", this_host.ip(), &router_web_port);
                    info!("Routing item to remote node: {}", web_host);
                    response = proxy_request(&web_host, "items", ProxyMethod::Post, Some(&item)).await.unwrap();
                } else {
                    info!("Routing to this node");
                    let now = now_millis();

                    let item = Item {
                        id: item.id.clone(),
                        message: item.message.clone(),
                        submitted_at: now.clone(),
                    };

                    item_entry = ItemEntry {
                        item: item.clone(),
                        status: ItemStatus::Active,
                        hlc: HLC {
                            timestamp: now.clone(),
                            counter: 0,
                        },
                    };
                    let this_node = node.address.clone();
                    {
                        let store = env.get_store().write().await;
                        node.add_items(&vec!(item_entry), &this_node, store).await;
                    }

                    // TODO: send to all replicas
                    send_gossip_single(
                        None,
                        socket.clone(),
                        node,
                        false,
                        env.clone()
                    ).await;

                    response = ItemSubmitResponse {
                        success: Some(vec![("id".to_string(), item.id.clone())].into_iter().collect()),
                        error: None,
                    };
                }
            } else {
                response = ItemSubmitResponse {
                    success: None,
                    error: Some(format!("Could not route to node, cluster not ready")),
                };
            }

            return Ok(warp::reply::json(&response));
        }
        _ => {
            let response = ItemSubmitResponse {
                success: None,
                error: Some(format!("Cannot submit item in current state")),
            };
            return Ok(warp::reply::json(&response));
        }
    }
}

async fn handle_get_task(
    req_path: String,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let memory = memory.read().await;

    match &*memory {
        node::NodeState::Joined(node) => {
            let item_id = req_path.split('/').last().unwrap_or_default().to_string();

            let routed_node = node.partition_map.route(&node.address, &item_id);
            info!("this node: {}, routed node: {:?}", &node.address, &routed_node);

            let store_ref = env.get_store().read().await;

            if let Some(item) = node.get_item(&item_id, &store_ref).await {
                match item.status {
                    ItemStatus::Active => {
                        let response = ItemSubmitResponse {
                            success: Some(vec![("item".to_string(), format!("{:?}", item))].into_iter().collect()),
                            error: None,
                        };
                        Ok(warp::reply::json(&response))
                    },
                    ItemStatus::Tombstone(_) => {
                        let response = ItemSubmitResponse {
                            success: None,
                            error: Some(format!("Task not found: {}", item_id)),
                        };
                        Ok(warp::reply::json(&response))
                    }
                }
            } else {
                let response = ItemSubmitResponse {
                    success: None,
                    error: Some(format!("Task not found: {}", item_id)),
                };
                Ok(warp::reply::json(&response))
            }
        }
        _ => {
            let response = ItemSubmitResponse {
                success: None,
                error: Some(format!("Cannot get item in current state")),
            };
            return Ok(warp::reply::json(&response));
        }
    }
}

async fn handle_get_tasks_count(
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>
) -> Result<impl warp::Reply, warp::Rejection> {
    let memory = memory.read().await;

    match &*memory {
        node::NodeState::Joined(node) => {
            let tasks_count = node.items_count(&env.get_store().read().await).await;
            let response = ItemSubmitResponse {
                success: Some(vec![("count".to_string(), tasks_count.to_string())].into_iter().collect()),
                error: None,
            };
            Ok(warp::reply::json(&response))
        },
        _ => {
            let response = ItemSubmitResponse {
                success: None,
                error: Some(format!("Cannot get items count in current state")),
            };
            Ok(warp::reply::json(&response))
        }
    }
}

async fn handle_remove_task(
    req_path: String,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut memory = memory.write().await;

    match &mut *memory {
        node::NodeState::Joined(node) => {
            let item_id = req_path.split('/').last().unwrap_or_default().to_string();
            let this_node_addr = node.get_address().clone();
            let this_node = node.get_node(&this_node_addr).unwrap().clone();
            let is_success = node.remove_item(&item_id.to_string(), &this_node_addr, &mut env.get_store().write().await).await;

            let routed_node = node.partition_map.route(&node.address, &item_id);
            info!("this node: {}, routed node: {:?}", &node.address, &routed_node);

            let response: ItemSubmitResponse;
            if let Some(node_address) = routed_node {
                if node.address != *node_address {
                    let this_host: SocketAddr = node_address.parse().unwrap();
                    let web_host = format!("{}:{}", this_host.ip(), this_node.web_port);
                    info!("Routing item to remote node: {}", web_host);

                    response = proxy_request::<()>(&web_host, format!("items/{}", item_id).as_str(), ProxyMethod::Delete, None).await.unwrap();
                } else {
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
                }
            } else {
                response = ItemSubmitResponse {
                    success: None,
                    error: Some(format!("Could not route to node, cluster not ready")),
                };
            }

            Ok(warp::reply::json(&response))
        }
        _ => {
            let response = ItemSubmitResponse {
                success: None,
                error: Some(format!("Cannot remove item in current state")),
            };
            Ok(warp::reply::json(&response))
        }
    }
}

async fn handle_update_task(
    item: ItemUpdate,
    req_path: String,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>
) -> Result<impl warp::Reply, warp::Rejection> {
    let item_id = req_path.split('/').last().unwrap_or_default().to_string();
    let mut memory = memory.write().await;

    match &mut *memory {
        node::NodeState::Joined(node) => {
            let response: ItemSubmitResponse;

            if node.get_item(&item_id, &env.get_store().read().await).await.is_some() {
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

                let node_address = node.get_address().clone();
                node.add_items(&vec!(new_item_entry), &node_address, env.get_store().write().await).await;
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
        },
        _ => {
            let response = ItemSubmitResponse {
                success: None,
                error: Some(format!("Cannot update item in current state")),
            };
            Ok(warp::reply::json(&response))
        }
    }
}

pub async fn web_server(
    local_web_addr: String,
    socket: Arc<UdpSocket>,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) {
    let post_item = warp::path!("items")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_socket(socket.clone()))
        .and(with_memory(memory.clone()))
        .and(with_env(env.clone()))
        .and_then(handle_post_task);

    let get_items_count = warp::path!("items")
        .and(warp::get())
        .and(with_memory(memory.clone()))
        .and(with_env(env.clone()))
        .and_then(handle_get_tasks_count);

    let get_item = warp::path!("items" / String)
        .and(warp::get())
        .and(with_memory(memory.clone()))
        .and(with_env(env.clone()))
        .and_then(handle_get_task);

    let remove_item = warp::path!("items" / String)
        .and(warp::delete())
        .and(with_memory(memory.clone()))
        .and(with_env(env.clone()))
        .and_then(handle_remove_task);

    let update_item = warp::path!("items" / String)
        .and(warp::put())
        .and(with_memory(memory.clone()))
        .and(warp::body::json())
        .and(with_env(env.clone()))
        .and_then(|req_path: String, memory: Arc<RwLock<NodeState>>, item: ItemUpdate, env: Arc<Env>| {
            handle_update_task(item, req_path, memory, env)
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
            id: "123".to_string(),
            message: "Test item".to_string(),
        };

        let req = client.post(format!("http://127.0.0.1:3002/items"))
            .json(&payload)
            .send()
            .await.unwrap();

        assert!(req.status().is_success());
    }
}