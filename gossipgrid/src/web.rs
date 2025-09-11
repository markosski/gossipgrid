use crate::env::Env;
use crate::gossip::{HLC, send_gossip_single};
use crate::node::{self, JoinedNode, NodeState};
use crate::{now_millis};
use crate::item::{Item, ItemEntry, ItemId, ItemStatus, ItemSubmit, ItemSubmitResponse, ItemUpdate};
use std::net::{AddrParseError, SocketAddr};
use log::{error,info};
use serde::{Deserialize, Serialize};

use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::{RwLock};
use warp::Filter;

const CANNOT_PERFORM_ACTION_IN_CURRENT_STATE: &str = "Cannot perform action in current state, is cluster ready?";

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

// TODO: handle errors where we attemp to send to another node
pub async fn try_route_request<T: Serialize, P: for<'de> Deserialize<'de>>(
    node: &JoinedNode,
    item_id: &ItemId,
    url: &str,
    method: ProxyMethod,
    body: Option<&T>
) -> Result<Option<P>, String> {
    info!("node={}; Proxying request: {}, {:?}, {:?}", node.get_address(), url, &method, serde_json::to_string(&body));

    let routed_node = node.partition_map.route(node.get_address(), item_id)
        .ok_or("Could not route to node".to_string())?;
    info!("node={}; routed node: {}", &node.address, &routed_node);

    if node.get_address() == &routed_node {
        info!("node={}; This is appropriate node, handling locally", node.get_address());
        return Ok(None);
    }

    let this_host: SocketAddr= routed_node.parse().map_err(|x: AddrParseError| x.to_string())?;

    let mut nodes = node.other_peers();
    nodes.retain(|k, _| k == &routed_node);

    if nodes.is_empty() {
        error!("node={}; Node for router address not found: {}, known nodes: {:?}", node.get_address(), &this_host.ip(), &nodes);
    }

    let router_web_port = nodes.iter().last().unwrap().1.web_port;
    let web_host = format!("{}:{}", this_host.ip(), &router_web_port);
    info!("node={}; Routing item to remote node: {}", node.get_address(), web_host);

    let client = reqwest::Client::new();
    let resp_builder = match method {
        ProxyMethod::Get => client.get(format!("http://{}/{}", web_host, url)),
        ProxyMethod::Post => client.post(format!("http://{}/{}", web_host, url))
            .json(&body.unwrap()),
        ProxyMethod::Put => client.put(format!("http://{}/{}", web_host, url)),
        ProxyMethod::Delete => client.delete(format!("http://{}/{}", web_host, url)),
    };

    let resp = resp_builder
        .send()
        .await.unwrap();

    let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
    let response_message = serde_json::from_slice::<P>(&bytes)
        .map_err(|e| e.to_string())?;

    Ok(Some(response_message))
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
            let routed_response = try_route_request::<ItemSubmit, ItemSubmitResponse>(node, &item.id, "items", ProxyMethod::Post, Some(&item)).await;

            match routed_response {
                Ok(Some(valid_routed_response)) => {
                    Ok(warp::reply::json(&valid_routed_response))
                }
                Ok(None) => {
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

                    send_gossip_single(
                        None,
                        socket.clone(),
                        node,
                        false,
                        env.clone()
                    ).await;

                    let response = ItemSubmitResponse {
                        success: Some(vec![("id".to_string(), item.id.clone())].into_iter().collect()),
                        error: None,
                    };
                    Ok(warp::reply::json(&response)) 
                }
                Err(err) => {
                    let response = ItemSubmitResponse {
                        success: None,
                        error: Some(err),
                    };
                    Ok(warp::reply::json(&response))
                }
            }
        }
        _ => {
            let response = ItemSubmitResponse {
                success: None,
                error: Some(CANNOT_PERFORM_ACTION_IN_CURRENT_STATE.to_string()),
            };
            Ok(warp::reply::json(&response))
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
            let routed_response = try_route_request::<ItemSubmit, ItemSubmitResponse>(node, &item_id, format!("items/{}", item_id).as_str(), ProxyMethod::Get, None).await;

            if let Ok(Some(valid_routed_response)) = routed_response {
                Ok(warp::reply::json(&valid_routed_response))

            } else if let Err(err) = routed_response {
                let response = ItemSubmitResponse {
                    success: None,
                    error: Some(format!("Routing error: {}", &err)),
                };
                Ok(warp::reply::json(&response))

            } else {
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
        }
        _ => {
            let response = ItemSubmitResponse {
                success: None,
                error: Some(CANNOT_PERFORM_ACTION_IN_CURRENT_STATE.to_string()),
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
                error: Some(CANNOT_PERFORM_ACTION_IN_CURRENT_STATE.to_string()),
            };
            Ok(warp::reply::json(&response))
        }
    }
}

async fn handle_remove_task(
    req_path: String,
    socket: Arc<UdpSocket>,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut memory = memory.write().await;

    match &mut *memory {
        node::NodeState::Joined(node) => {
            let item_id = req_path.split('/').last().unwrap_or_default().to_string();
            let routed_response = try_route_request::<ItemSubmit, ItemSubmitResponse>(node, &item_id, format!("items/{}", item_id).as_str(), ProxyMethod::Delete, None).await;

            match routed_response {
                Ok(Some(valid_routed_response)) => {
                    Ok(warp::reply::json(&valid_routed_response))
                }
                Ok(None) => {
                    let response: ItemSubmitResponse;
                    let this_node_addr = node.get_address().clone();
                    let is_success = node.remove_item(&item_id.to_string(), &this_node_addr, &mut env.get_store().write().await).await;
                    if is_success {
                        response = ItemSubmitResponse {
                            success: Some(vec![("id".to_string(), item_id)].into_iter().collect()),
                            error: None,
                        };

                        send_gossip_single(
                            None,
                            socket.clone(),
                            node,
                            false,
                            env.clone()
                        ).await;
                    } else {
                        response = ItemSubmitResponse {
                            success: None,
                            error: Some(format!("Task not found: {}", item_id)),
                        };
                    }
                    Ok(warp::reply::json(&response))
                }
                Err(err) => {
                    let response = ItemSubmitResponse {
                        success: None,
                        error: Some(err),
                    };
                    Ok(warp::reply::json(&response))
                }
            }
        }
        _ => {
            let response = ItemSubmitResponse {
                success: None,
                error: Some(CANNOT_PERFORM_ACTION_IN_CURRENT_STATE.to_string()),
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
            let routed_response = try_route_request::<ItemUpdate, ItemSubmitResponse>(node, &item_id, format!("items/{}", item_id).as_str(), ProxyMethod::Put, Some(&item)).await;

            match routed_response {
                Ok(Some(valid_routed_response)) => {
                    Ok(warp::reply::json(&valid_routed_response))
                }
                Ok(None) => {
                    info!("Routing to this node");
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
                }
                Err(err) => {
                    let response = ItemSubmitResponse {
                        success: None,
                        error: Some(format!("Routing error: {}", &err)),
                    };
                    Ok(warp::reply::json(&response))
                }
            }
        },
        _ => {
            let response = ItemSubmitResponse {
                success: None,
                error: Some(CANNOT_PERFORM_ACTION_IN_CURRENT_STATE.to_string()),
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
        .and(with_socket(socket.clone()))
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