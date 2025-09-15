use tokio::sync::RwLock;

use crate::{event::EventPublisher, store::Store};

pub struct Env {
    pub store: RwLock<Box<dyn Store>>,
    pub event_publisher: RwLock<Box<dyn EventPublisher>>,
}

impl Env {
    pub fn new(store: Box<dyn Store>, event_publisher: Box<dyn EventPublisher>) -> Self {
        Env {
            store: RwLock::new(store), 
            event_publisher: RwLock::new(event_publisher) }
    } 

    pub fn get_store(&self) -> &RwLock<Box<dyn Store>> {
        &self.store
    }
    
    pub fn get_event_publisher(&self) -> &RwLock<Box<dyn EventPublisher>> {
        &self.event_publisher
    } 
}
