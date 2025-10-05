use crate::{event::EventPublisher, store::Store};

pub struct Env {
    pub store: Box<dyn Store + Send + Sync>,
    pub event_publisher: Box<dyn EventPublisher + Send + Sync>,
}

impl Env {
    pub fn new(store: Box<dyn Store>, event_publisher: Box<dyn EventPublisher>) -> Self {
        Env {
            store: store,
            event_publisher: event_publisher,
        }
    }

    pub fn get_store(&self) -> &(dyn Store + Send + Sync) {
        &*self.store
    }

    pub fn get_event_publisher(&self) -> &(dyn EventPublisher + Send + Sync) {
        &*self.event_publisher
    }
}
