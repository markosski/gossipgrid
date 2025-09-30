use crate::env::Env;
use crate::gossip::{HLC, send_gossip_single};
use crate::item::{
    Item, ItemEntry, ItemGenericResponseEnvelope, ItemOpsResponseEnvelope, ItemStatus, ItemCreateUpdate
};
use crate::node::{self, JoinedNode, NodeState};
use crate::store::{StorageKey, PartitionKey, RangeKey};
use crate::{now_millis};
use log::{error, info};
use serde::{Deserialize, Serialize};
use warp::filters::path::FullPath;
use std::collections::HashMap;
use std::net::{AddrParseError, SocketAddr};

use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use warp::Filter;

const CANNOT_PERFORM_ACTION_IN_CURRENT_STATE: &str =
    "Cannot perform action in current state, is cluster ready?";

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
    Delete,
}

pub async fn try_route_request<T: Serialize, P: for<'de> Deserialize<'de>>(
    node: &JoinedNode,
    item_id: &String,
    url: &str,
    method: ProxyMethod,
    body: Option<&T>,
) -> Result<Option<P>, String> {
    info!(
        "node={}; Try proxying request: {}, {:?}, {:?}",
        node.get_address(),
        url,
        &method,
        serde_json::to_string(&body)
    );

    let routed_node = node
        .partition_map
        .route(node.get_address(), item_id)
        .ok_or("Could not route to node".to_string())?;
    info!("node={}; routed node: {}", &node.address, &routed_node);

    if node.get_address() == &routed_node {
        info!(
            "node={}; This is appropriate node, handling locally",
            node.get_address()
        );
        return Ok(None);
    }

    let this_host: SocketAddr = routed_node
        .parse()
        .map_err(|x: AddrParseError| x.to_string())?;

    let mut nodes = node.other_peers();
    nodes.retain(|k, _| k == &routed_node);

    if nodes.is_empty() {
        error!(
            "node={}; Node for router address not found: {}, known nodes: {:?}",
            node.get_address(),
            &this_host.ip(),
            &nodes
        );
    }

    let router_web_port = nodes
        .iter()
        .last()
        .map(|n| n.1.web_port)
        .ok_or("No web port found")?;
    let web_host = format!("{}:{}", this_host.ip(), &router_web_port);
    info!(
        "node={}; Routing item to remote node: {}",
        node.get_address(),
        web_host
    );

    let client = reqwest::Client::new();
    // TODO: remove hard coded scheme
    let resp_builder = match method {
        ProxyMethod::Get => client.get(format!("http://{}{}", web_host, url)),
        ProxyMethod::Post => client
            .post(format!("http://{}{}", web_host, url))
            .json(&body.expect("Body is required for POST")),
        ProxyMethod::Delete => client.delete(format!("http://{}{}", web_host, url)),
    };

    let resp = resp_builder
        .send()
        .await
        .or(Err("Failed to send request".to_string()))?;

    let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
    let response_message = serde_json::from_slice::<P>(&bytes).map_err(|e| e.to_string())?;

    Ok(Some(response_message))
}

// TODO: Implement the redirect filter for all endpoints
async fn handle_post_item(
    req_path: FullPath,
    item_submit: ItemCreateUpdate,
    socket: Arc<UdpSocket>,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut memory = memory.write().await;
    match &mut *memory {
        node::NodeState::Joined(node) => {
            let routed_response = try_route_request::<ItemCreateUpdate, ItemOpsResponseEnvelope>(
                node,
                &item_submit.partition_key,
                req_path.as_str(),
                ProxyMethod::Post,
                Some(&item_submit),
            )
            .await;
            let storage_key = StorageKey::new(
                PartitionKey(item_submit.partition_key.clone()),
                item_submit.range_key.map(|rk| RangeKey(rk))
            );

            match routed_response {
                Ok(Some(valid_routed_response)) => Ok(warp::reply::json(&valid_routed_response)),
                Ok(None) => {
                    info!("Routing to this node");
                    let now = now_millis();

                    let item = Item {
                        message: item_submit.message.clone().as_bytes().to_vec(),
                        status: ItemStatus::Active,
                        hlc: HLC {
                            timestamp: now.clone(),
                            counter: 0,
                        },
                    };

                    let item_entry = ItemEntry {
                        storage_key: storage_key,
                        item: item.clone(),
                    };

                    let this_node = node.address.clone();
                    {
                        let store = env.get_store().write().await;
                        match node.add_items(&vec![item_entry.clone()], &this_node, store).await {
                            Ok(_) => (),
                            Err(e) => {
                                let response = ItemOpsResponseEnvelope {
                                    success: None,
                                    error: Some(format!("Item submission error: {}", e)),
                                };
                                return Ok(warp::reply::json(&response));
                            }
                        }
                    }

                    send_gossip_single(None, socket.clone(), node, false, env.clone()).await;

                    let response = ItemOpsResponseEnvelope {
                        success: Some(
                            vec![item_entry]
                        ),
                        error: None,
                    };
                    Ok(warp::reply::json(&response))
                }
                Err(err) => {
                    let response = ItemOpsResponseEnvelope {
                        success: None,
                        error: Some(err),
                    };
                    Ok(warp::reply::json(&response))
                }
            }
        }
        _ => {
            let response = ItemOpsResponseEnvelope {
                success: None,
                error: Some(CANNOT_PERFORM_ACTION_IN_CURRENT_STATE.to_string()),
            };
            Ok(warp::reply::json(&response))
        }
    }
}

async fn handle_get_items(
    store_key: String,
    req_path: FullPath,
    params: HashMap<String, String>,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let memory = memory.read().await;

    match &*memory {
        node::NodeState::Joined(node) => {
            let storage_key: StorageKey = store_key.parse().unwrap();
            let storage_key_string = storage_key.to_string();

            let query_with_q = params.iter().fold("".to_string(), |acc, (k, v)| {
                if acc.is_empty() {
                    format!("?{}={}", k, v)
                } else {
                    format!("{}&{}={}", acc, k, v)
                }
            });

            let routed_response = try_route_request::<ItemCreateUpdate, ItemOpsResponseEnvelope>(
                node,
                &storage_key_string,
                format!("{}{}", req_path.as_str(), query_with_q).as_str(),
                ProxyMethod::Get,
                None,
            )
            .await;

            let limit = params.get("limit").map(|v| v.parse::<usize>().unwrap_or(10)).unwrap_or(10);

            if let Ok(Some(valid_routed_response)) = routed_response {
                Ok(warp::reply::json(&valid_routed_response))
            } else if let Err(err) = routed_response {
                let response = ItemOpsResponseEnvelope {
                    success: None,
                    error: Some(format!("Routing error: {}", &err)),
                };
                Ok(warp::reply::json(&response))
            } else {
                let store_ref = env.get_store().read().await;

                let item_entries = match node.get_items(limit, &storage_key, &store_ref).await {
                    Ok(item_entry) => item_entry,
                    Err(e) => {
                        let response = ItemOpsResponseEnvelope {
                            success: None,
                            error: Some(format!("Item retrieval error: {}", e)),
                        };
                        return Ok(warp::reply::json(&response));
                    }
                };

                if item_entries.len() > 0 {
                    let response = ItemOpsResponseEnvelope {
                        success: Some(
                            item_entries 
                        ),
                        error: None,
                    };
                    Ok(warp::reply::json(&response))
                } else {
                    let response = ItemOpsResponseEnvelope {
                        success: None,
                        error: Some(format!("No items found: {}", &storage_key_string)),
                    };
                    Ok(warp::reply::json(&response))
                }
            }
        }
        _ => {
            let response = ItemOpsResponseEnvelope {
                success: None,
                error: Some(CANNOT_PERFORM_ACTION_IN_CURRENT_STATE.to_string()),
            };
            return Ok(warp::reply::json(&response));
        }
    }
}

async fn handle_get_item_count(
    memory: Arc<RwLock<NodeState>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let memory = memory.read().await;

    match &*memory {
        node::NodeState::Joined(node) => {
            let tasks_count = node.items_count();
            let response = ItemGenericResponseEnvelope {
                success: Some(
                    vec![("count".to_string(), tasks_count.to_string())]
                        .into_iter()
                        .collect(),
                ),
                error: None,
            };
            Ok(warp::reply::json(&response))
        }
        _ => {
            let response = ItemGenericResponseEnvelope {
                success: None,
                error: Some(CANNOT_PERFORM_ACTION_IN_CURRENT_STATE.to_string()),
            };
            Ok(warp::reply::json(&response))
        }
    }
}

async fn handle_remove_item(
    store_key: String,
    req_path: FullPath,
    socket: Arc<UdpSocket>,
    memory: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut memory = memory.write().await;

    match &mut *memory {
        node::NodeState::Joined(node) => {
            let storage_key: StorageKey = store_key.parse().unwrap();
            let storage_key_string = storage_key.to_string();
            let routed_response = try_route_request::<ItemCreateUpdate, ItemOpsResponseEnvelope>(
                node,
                &storage_key_string,
                req_path.as_str(),
                ProxyMethod::Delete,
                None,
            )
            .await;

            match routed_response {
                Ok(Some(valid_routed_response)) => Ok(warp::reply::json(&valid_routed_response)),
                Ok(None) => {
                    let response: ItemGenericResponseEnvelope;
                    let this_node_addr = node.get_address().clone();

                    let is_success = match node
                        .remove_item(
                            &storage_key,
                            &this_node_addr,
                            &mut env.get_store().write().await,
                        )
                        .await {
                            Ok(_) => true,
                            Err(e) => {
                                error!("Error removing item: {}", e);
                                return Ok(warp::reply::json(
                                &ItemGenericResponseEnvelope {
                                    success: None,
                                    error: Some(format!("Item not found: {}", &storage_key_string)),
                                }));
                            }
                        };

                    if is_success {
                        response = ItemGenericResponseEnvelope {
                            success: Some(vec![("id".to_string(), storage_key_string.clone())].into_iter().collect()),
                            error: None,
                        };

                        send_gossip_single(None, socket.clone(), node, false, env.clone()).await;
                    } else {
                        response = ItemGenericResponseEnvelope {
                            success: None,
                            error: Some(format!("Item not found: {}", &storage_key_string)),
                        };
                    }
                    Ok(warp::reply::json(&response))
                }
                Err(err) => {
                    let response = ItemGenericResponseEnvelope {
                        success: None,
                        error: Some(err),
                    };
                    Ok(warp::reply::json(&response))
                }
            }
        }
        _ => {
            let response = ItemOpsResponseEnvelope {
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
        .and(warp::path::full())
        .and(warp::body::json())
        .and(with_socket(socket.clone()))
        .and(with_memory(memory.clone()))
        .and(with_env(env.clone()))
        .and_then(handle_post_item);

    let get_items_count = warp::path!("items")
        .and(warp::get())
        .and(with_memory(memory.clone()))
        .and_then(handle_get_item_count);

    let get_items = warp::path!("items" / String)
        .and(warp::get())
        .and(warp::path::full())
        .and(warp::query::<HashMap<String, String>>())
        .and(with_memory(memory.clone()))
        .and(with_env(env.clone()))
        .and_then(handle_get_items);

    let remove_item = warp::path!("items" / String)
        .and(warp::delete())
        .and(warp::path::full())
        .and(with_socket(socket.clone()))
        .and(with_memory(memory.clone()))
        .and(with_env(env.clone()))
        .and_then(handle_remove_item);

    let address = local_web_addr
        .parse::<SocketAddr>()
        .expect("Failed to parse address for web server");
    warp::serve(
        post_item
            .or(get_items)
            .or(get_items_count)
            .or(remove_item)
    )
    .run(address)
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn simple_test() {
        let client = reqwest::Client::new();

        let payload = ItemCreateUpdate {
            partition_key: "123".to_string(),
            range_key: Some("range1".to_string()),
            message: "Test item".to_string(),
        };

        let req = client
            .post(format!("http://127.0.0.1:3002/items"))
            .json(&payload)
            .send()
            .await
            .unwrap();

        assert!(req.status().is_success());
    }
}
