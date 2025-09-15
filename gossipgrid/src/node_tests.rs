use std::collections::HashMap;
use supper::*;

#[test]
fn test_merge_tasks() {
    let mut local_memory = NodeMemory {
        known_peers: HashMap::new(),
        peers_hlc: HLC { timestamp: 100, counter: 0 },
        tasks: HashMap::new(),
    };

    let remote_tasks = {
        let mut tasks = HashMap::new();
        tasks.insert(
            "node1".to_string(),
            TaskSet {
                tasks: vec![Task {
                    id: "task1".to_string(),
                    message: "task1 message".to_string(),
                    submitted_at: 100,
                    status: TaskStatus::Pending,
                }],
                hlc: HLC { timestamp: 101, counter: 0 },
            },
        );
        tasks
    };

    local_memory.tasks.insert(
        "node1".to_string(),
        TaskSet {
            tasks: vec![Task {
                id: "task2".to_string(),
                message: "task2 message".to_string(),
                submitted_at: 100,
                status: TaskStatus::Pending,
            }],
            hlc: HLC { timestamp: 100, counter: 0 },
        },
    );

    local_memory.merge_tasks(&remote_tasks);

    let merged_task_set = local_memory.tasks.get("node1").unwrap();
    assert_eq!(merged_task_set.tasks.len(), 1);
    assert_eq!(merged_task_set.tasks[0].id, "task1");
    assert_eq!(merged_task_set.hlc.timestamp, 101);
}