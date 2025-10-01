use thiserror::Error;

use crate::gossip::HLC;



/// Send sync data
/// true - there was data to send
/// false - no more data to send
async fn sync_send(item_hlc: HLC) -> Result<bool, SyncTransportError> {
    Ok(true)
}

#[derive(Error, Debug)]                                                   
pub enum SyncTransportError {
    #[error("Error sending sync: {0}")]                   
    SyndSendError(String),
}