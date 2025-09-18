use gossipgrid::item::ItemSubmitResponse;

mod helpers;

#[tokio::test]
async fn test_publish_and_retrieve_item() {
    env_logger::init();

    let nodes = helpers::start_test_cluster(3, 3).await;

    let client = reqwest::Client::new();
    let _ = client
        .post("http://localhost:3001/items")
        .body(r#"{"id": "123", "message": "foo1"}"#)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let res = client
        .get("http://localhost:3002/items/123")
        .send()
        .await
        .unwrap();

    let response: ItemSubmitResponse =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();
    let id = response.success.unwrap().get("item").unwrap().to_string();

    assert!(id.contains("123"));

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}

#[tokio::test]
async fn test_publish_and_delete_item() {
    env_logger::init();

    let nodes = helpers::start_test_cluster(3, 3).await;

    let client = reqwest::Client::new();
    let _ = client
        .post("http://localhost:3001/items")
        .body(r#"{"id": "123", "message": "foo1"}"#)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let res = client
        .get("http://localhost:3002/items/123")
        .send()
        .await
        .unwrap();

    let response: ItemSubmitResponse =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();
    let id = response.success.unwrap().get("item").unwrap().to_string();

    assert!(id.contains("123"));

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

    let response: ItemSubmitResponse =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();

    assert!(response.error.unwrap().contains("Item not found"));

    helpers::stop_nodes(nodes.into_iter().map(|n| n.0).collect()).await;
}

#[tokio::test]
async fn test_publish_and_update_item() {
    env_logger::init();

    let nodes = helpers::start_test_cluster(3, 3).await;

    let client = reqwest::Client::new();
    let _ = client
        .post("http://localhost:3001/items")
        .body(r#"{"id": "123", "message": "foo1"}"#)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let res = client
        .get("http://localhost:3002/items/123")
        .send()
        .await
        .unwrap();

    let response: ItemSubmitResponse =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();
    let id = response.success.unwrap().get("item").unwrap().to_string();

    assert!(id.contains("123"));

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let _ = client
        .post("http://localhost:3002/items")
        .body(r#"{"id": "123", "message": "foo2"}"#)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let res = client
        .get("http://localhost:3003/items/123")
        .send()
        .await
        .unwrap();

    let response: ItemSubmitResponse =
        serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();
    let id = response.success.unwrap().get("item").unwrap().to_string();

    assert!(id.contains("foo2"));

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
