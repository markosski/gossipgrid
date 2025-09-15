use gossipgrid::item::{ItemSubmitResponse};

mod helpers;

#[tokio::test]
async fn test_publish_and_retrieve_item() {
    env_logger::init();

    let nodes = helpers::start_test_cluster().await;

    let client = reqwest::Client::new();
    let _ = client.post("http://localhost:3001/items")
        .body(r#"{"id": "123", "message": "foo1"}"#)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let res = client.get("http://localhost:3002/items/123")
        .send()
        .await
        .unwrap();

    let response: ItemSubmitResponse = serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();
    let id = response.success.unwrap().get("item").unwrap().to_string();

    assert!(id.contains("123"));

    helpers::stop_nodes(nodes).await;
}

#[tokio::test]
async fn test_publish_and_delete_item() {
    env_logger::init();

    let nodes = helpers::start_test_cluster().await;

    let client = reqwest::Client::new();
    let _ = client.post("http://localhost:3001/items")
        .body(r#"{"id": "123", "message": "foo1"}"#)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let _ = client.delete("http://localhost:3002/items/123")
        .send()
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let res = client.get("http://localhost:3002/items/123")
        .send()
        .await
        .unwrap();

    let response: ItemSubmitResponse = serde_json::from_str(res.text().await.unwrap().as_str()).unwrap();

    assert!(response.error.unwrap().contains("Item not found"));

    helpers::stop_nodes(nodes).await;
}