use serde::{Deserialize, Serialize};
use tokio::fs::File;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub address_from: String,
    pub address_to: String,
    pub message_type: String,
    pub data: serde_json::Value,
    pub timestamp: u64,
}

#[async_trait::async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&mut self, msg: &Event);
}

pub struct EventPublisherFileLogger {
    pub file_path: String,
    file: File,
}

impl EventPublisherFileLogger {
    pub async fn new(file_path: String) -> Self {
        use tokio::fs::OpenOptions;

        tokio::fs::remove_file(&file_path).await.ok();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .await
            .expect("Failed to open event log file");

        EventPublisherFileLogger {
            file_path,
            file: file,
        }
    }
}

#[async_trait::async_trait]
impl EventPublisher for EventPublisherFileLogger {
    async fn publish(&mut self, msg: &Event) {
        use tokio::io::AsyncWriteExt;

        let msg_json = serde_json::to_string(msg).expect("Failed to serialize event");

        if let Err(e) = self.file.write_all((msg_json + "\n").as_bytes()).await {
            eprintln!("Failed to write to event log file: {}", e);
        }
    }
}
