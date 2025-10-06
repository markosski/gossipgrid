use thiserror::Error;

use crate::{gossip::HLC, store::Store};



/// Send sync data
/// true - there was data to send
/// false - no more data to send
async fn sync_send(item_hlc: HLC, sync_node_address: &str, sync_data_size: u16, store: &dyn Store) -> Result<bool, SyncTransportError> {
    Ok(true)
}

#[derive(Error, Debug)]                                                   
pub enum SyncTransportError {
    #[error("Error sending sync: {0}")]                   
    SyndSendError(String),
}