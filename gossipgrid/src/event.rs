use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub address_from: String,
    pub address_to: String,
    pub message_type: String,
    pub data: serde_json::Value,
    pub timestamp: u64,
}

pub struct EventPublisherFileLogger {
    pub file_path: String,
}

#[async_trait::async_trait]
pub trait EventPublisher: Send + Sync{
    async fn publish(&self, msg: &Event);
}

#[async_trait::async_trait]
impl EventPublisher for EventPublisherFileLogger {
    async fn publish(&self, msg: &Event) {
        use tokio::fs::OpenOptions;
        use tokio::io::AsyncWriteExt;

        let msg_json = serde_json::to_string(msg).expect("Failed to serialize event");

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file_path)
            .await
            .expect("Failed to open event log file");

        if let Err(e) = file.write_all((msg_json + "\n").as_bytes()).await {
            eprintln!("Failed to write to event log file: {}", e);
        }
    }
}