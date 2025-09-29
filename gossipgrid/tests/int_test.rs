use gossipgrid::{
    item::ItemOpsResponseEnvelope,
    node::{NodeState},
};

mod helpers;

#[tokio::test]
async fn test_publish_and_retrieve_item() {
    env_logger::init();

    log::info!("Starting test_publish_and_retrieve_item");

    let nodes = helpers::start_test_cluster(3, 3).await;

    let client = reqwest::Client::new();
    let _ = client
        .post("http://localhost:3001/items")
        .body(r#"{"partition_key": "123", "range_key": "456", "message": "foo1"}"#)
        .send()
        .await
        .unwrap();

    // Submitting another item to ensure we are fetching the correct one
    let _ = client
        .post("http://localhost:3001/items")
        .body(r#"{"partition_key": "123", "range_key": "457", "message": "foo1"}"#)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let res = client
        .get("http://localhost:3002/items/123_456")
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success());

    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();

    let item_count = response.success.as_ref().unwrap().len().clone();
    let item_id = response.success.unwrap().get(0).unwrap().storage_key.to_string();

    assert!(item_count == 1);
    assert!(item_id.contains("123_456"));

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let node_guard = nodes[2].1.read().await;
    let node = match &*node_guard {
        NodeState::Joined(state) => state,
        _ => panic!("Node is not in Joined state"),
    };

    let count = node.items_count();
    assert_eq!(count, 2);
    drop(node_guard);

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}

#[tokio::test]
async fn test_publish_and_retrieve_many_item() {
    env_logger::init();

    log::info!("Starting test_publish_and_retrieve_item");

    let nodes = helpers::start_test_cluster(3, 3).await;

    let client = reqwest::Client::new();
    let _ = client
        .post("http://localhost:3001/items")
        .body(r#"{"partition_key": "123", "range_key": "456", "message": "foo1"}"#)
        .send()
        .await
        .unwrap();

    let _ = client
        .post("http://localhost:3002/items")
        .body(r#"{"partition_key": "123", "range_key": "457", "message": "foo2"}"#)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let res = client
        .get("http://localhost:3002/items/123?limit=10")
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success());

    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();
    let items = response.success.unwrap();

    assert!(items.len() == 2);

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}

#[tokio::test]
async fn test_publish_and_delete_item() {
    env_logger::init();

    let nodes = helpers::start_test_cluster(3, 3).await;

    let client = reqwest::Client::new();
    let _ = client
        .post("http://localhost:3001/items")
        .body(r#"{"partition_key": "123", "message": "foo1"}"#)
        .send()
        .await
        .unwrap();

    // verify item is created
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let res = client
        .get("http://localhost:3002/items/123")
        .send()
        .await
        .unwrap();

    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();
    let id = response.success.unwrap().get(0).unwrap().storage_key.to_string();

    assert!(id.contains("123"));

    // verify item is deleted
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let _ = client
        .delete("http://localhost:3002/items/123")
        .send()
        .await
        .unwrap();

    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let res = client
        .get("http://localhost:3001/items/123")
        .send()
        .await
        .unwrap();

    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();

    assert!(response.error.unwrap().contains("No items found"));

    // verify cluster item count
    let node_guard = nodes[2].1.read().await;
    let node = match &*node_guard {
        NodeState::Joined(state) => state,
        _ => panic!("Node is not in Joined state"),
    };

    let count = node.items_count();
    assert_eq!(count, 0);
    drop(node_guard);

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}

#[tokio::test]
async fn test_publish_and_update_item() {
    env_logger::init();

    let nodes = helpers::start_test_cluster(3, 3).await;

    let client = reqwest::Client::new();
    let _ = client
        .post("http://localhost:3001/items")
        .body(r#"{"partition_key": "123", "message": "foo1"}"#)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let res = client
        .get("http://localhost:3002/items/123")
        .send()
        .await
        .unwrap();

    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();
    let id = response.success.unwrap().get(0).unwrap().storage_key.to_string();

    assert!(id.contains("123"));

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let _ = client
        .post("http://localhost:3002/items")
        .body(r#"{"partition_key": "123", "message": "foo2"}"#)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let res = client
        .get("http://localhost:3003/items/123")
        .send()
        .await
        .unwrap();

    let response: ItemOpsResponseEnvelope =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();
    let item_entry = response.success.unwrap().get(0).unwrap().clone();
    let message = String::from_utf8(item_entry.item.message.clone()).unwrap();

    assert!(message.contains("foo2"));

    // verify node received ack and updated its delta state
    // none of the nodes should have the item

    // let node_memory = &nodes[2].1;
    // let guard = node_memory.read().await;
    // if let gossipgrid::node::NodeState::Joined(state) = &*guard {
    //     let item = state.get_delta_state().get("123").unwrap();
    //     assert_eq!(item.message, "foo2");
    // } else {
    //     panic!("Node is not in Joined state");
    // }

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}
