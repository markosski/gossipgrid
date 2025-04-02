#![allow(unused)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Instant, Duration};
use bincode::{Encode, Decode};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Encode, Decode, Deserialize, Serialize)]
pub struct Item {
    pub id: String,
    pub message: String, // Example metadata
    pub submitted_at: u64,
    pub status: ItemStatus,
}

#[derive(Debug, Clone, Encode, Decode, Deserialize, Serialize)]
pub struct ItemSubmit {
    pub id: String,
    pub message: String
}

#[derive(Debug, Clone, Encode, Decode, Deserialize, Serialize)]
pub enum ItemStatus {
    Pending,
    Running(String), // Running on node "X"
    Completed,
}

pub struct ItemQueue {
    queue: VecDeque<Item>,
}

impl ItemQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }
    
    pub fn from_vec(tasks: Vec<Item>) -> Self {
        Self {
            queue: tasks.into_iter().collect(),
        }
    }

    pub fn to_vec(&self) -> Vec<Item> {
        self.queue.iter().cloned().collect()
    }

    pub fn add_item(&mut self, task: Item) {
        self.queue.push_back(task);
    }

    pub fn get_active_items(&self) -> Vec<Item> {
        self.queue.iter()
            .filter(|task| !matches!(task.status, ItemStatus::Running(_)))
            .cloned()
            .collect()
    }

    pub fn update_item(&mut self, new_task: Item) {
        if let Some(task) = self.queue.iter_mut().find(|t| t.id == new_task.id) {
            *task = new_task.clone();
        } else {
            self.add_item(new_task);
        }
    }
    
    pub fn replace_item(&mut self, tasks: Vec<Item>) {
        self.queue = tasks.into_iter().collect();
    }

    pub fn reassign_failed_item(&mut self, failed_node: &str) {
        for task in self.queue.iter_mut() {
            if let ItemStatus::Running(node) = &task.status {
                if node == failed_node {
                    task.status = ItemStatus::Pending;
                }
            }
        }
    }
}

// Shared state for all nodes
type SharedTaskQueue = Arc<Mutex<ItemQueue>>;
