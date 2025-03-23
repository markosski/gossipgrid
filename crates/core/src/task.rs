#![allow(unused)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Instant, Duration};
use bincode::{Encode, Decode};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Encode, Decode)]
pub struct Task {
    pub id: String,
    pub message: String, // Example metadata
    pub submitted_at: u64,
    pub status: TaskStatus,
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum TaskStatus {
    Pending,
    Running(String), // Running on node "X"
    Completed,
}

pub struct TaskQueue {
    queue: VecDeque<Task>,
}

impl TaskQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }
    
    pub fn from_vec(tasks: Vec<Task>) -> Self {
        Self {
            queue: tasks.into_iter().collect(),
        }
    }

    pub fn to_vec(&self) -> Vec<Task> {
        self.queue.iter().cloned().collect()
    }

    pub fn add_task(&mut self, task: Task) {
        self.queue.push_back(task);
    }

    pub fn get_active_tasks(&self) -> Vec<Task> {
        self.queue.iter()
            .filter(|task| !matches!(task.status, TaskStatus::Running(_)))
            .cloned()
            .collect()
    }

    pub fn update_task(&mut self, new_task: Task) {
        if let Some(task) = self.queue.iter_mut().find(|t| t.id == new_task.id) {
            *task = new_task.clone();
        } else {
            self.add_task(new_task);
        }
    }
    
    pub fn replace_tasks(&mut self, tasks: Vec<Task>) {
        self.queue = tasks.into_iter().collect();
    }

    pub fn reassign_failed_tasks(&mut self, failed_node: &str) {
        for task in self.queue.iter_mut() {
            if let TaskStatus::Running(node) = &task.status {
                if node == failed_node {
                    task.status = TaskStatus::Pending;
                }
            }
        }
    }
}

// Shared state for all nodes
type SharedTaskQueue = Arc<Mutex<TaskQueue>>;

pub fn process_task() {

}