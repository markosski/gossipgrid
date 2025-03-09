#![allow(unused)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Instant, Duration};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Task {
    id: String,
    metadata: String, // Example metadata
    submitted_at: u64,
    status: TaskStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum TaskStatus {
    Pending,
    Running(String), // Running on node "X"
    Completed,
}

struct TaskQueue {
    queue: VecDeque<Task>,
}

impl TaskQueue {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }

    fn add_task(&mut self, task: Task) {
        self.queue.push_back(task);
    }

    fn get_next_task(&mut self, node_id: &str) -> Option<Task> {
        if let Some(task) = self.queue.front_mut() {
            if let TaskStatus::Pending = task.status {
                task.status = TaskStatus::Running(node_id.to_string());
                return Some(task.clone());
            }
        }
        None
    }

    fn complete_task(&mut self, task_id: &str) {
        if let Some(pos) = self.queue.iter().position(|t| t.id == task_id) {
            self.queue.remove(pos);
        }
    }
}

// Shared state for all nodes
type SharedTaskQueue = Arc<Mutex<TaskQueue>>;