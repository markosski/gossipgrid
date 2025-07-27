use tokio::sync::RwLock;

use crate::{event::EventPublisher, store::Store};

pub trait Env: Sync + Send{
    fn get_store(&self) -> &RwLock<Box<dyn Store>>;
    fn get_event_publisher(&self) -> &Box<dyn EventPublisher>;

}

pub struct EnvDeps {
    pub store: RwLock<Box<dyn Store>>,
    pub event_publisher: Box<dyn EventPublisher>,
}

impl EnvDeps {
    pub fn new(store: Box<dyn Store>, event_publisher: Box<dyn EventPublisher>) -> Self {
        EnvDeps {
            store: RwLock::new(store), 
            event_publisher: event_publisher }
    } 
}

impl Env for EnvDeps  {
    fn get_store(&self) -> &RwLock<Box<dyn Store>> {
        &self.store
    }
    
    fn get_event_publisher(&self) -> &Box<dyn EventPublisher> {
        &self.event_publisher
    } 
}
