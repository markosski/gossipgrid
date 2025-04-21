use crate::gossip::{NodeMemory, receive_gossip, send_gossip, send_gossip_single};
use crate::web;
use log::info;
use names::Generator;

use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

// Main entry point for the node
pub async fn start_node(web_addr: String, local_addr: String, node_memory: Arc<Mutex<NodeMemory>>) {
    // Generate name for this node
    let mut generator = Generator::default();
    let node_name = generator.next().unwrap();
    let sync_flag = Arc::new(Mutex::new(true));

    info!(name=node_name.as_str(); "node={}; Starting application: {}", &node_name, &local_addr);

    // Initialize socket
    let socket = Arc::new(UdpSocket::bind(&local_addr).await.unwrap());

    // Fire and forget gossip tasks
    let _ = tokio::spawn(send_gossip(
        node_name.clone(),
        local_addr.clone(),
        socket.clone(),
        node_memory.clone(),
        sync_flag.clone(),
    ));
    let _ = tokio::spawn(receive_gossip(
        node_name.clone(),
        socket.clone(),
        node_memory.clone(),
        sync_flag.clone(),
    ));

    // Initiate HTTP server
    info!(name=node_name.as_str(); "node={}; Starting Web Server: {}", &node_name, &web_addr);
    web::web_server(web_addr.clone(), socket.clone(), node_memory.clone()).await;
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::collections::HashMap;
//     use crate::gossip::HLC;
//     use crate::item::{Item, ItemStatus};

//     #[test]
//     fn test_merge_tasks() {
//         let mut local_memory = NodeMemory {
//             known_peers: HashMap::new(),
//             peers_hlc: HLC { timestamp: 100, counter: 0 },
//             items: HashMap::new(),
//         };

//         let remote_tasks = {
//             let mut tasks = HashMap::new();
//             tasks.insert(
//                 "node1".to_string(),
//                 TaskSet {
//                     tasks: vec![Item {
//                         id: "task1".to_string(),
//                         message: "task1 message".to_string(),
//                         submitted_at: 100,
//                         status: ItemStatus::Pending,
//                     }],
//                     hlc: HLC { timestamp: 101, counter: 0 },
//                 },
//             );
//             tasks
//         };

//         local_memory.items.insert(
//             "node1".to_string(),
//             TaskSet {
//                 tasks: vec![Item {
//                     id: "task2".to_string(),
//                     message: "task2 message".to_string(),
//                     submitted_at: 100,
//                     status: ItemStatus::Pending,
//                 }],
//                 hlc: HLC { timestamp: 100, counter: 0 },
//             },
//         );

//         local_memory.merge_tasks(&remote_tasks);

//         let merged_task_set = local_memory.items.get("node1").unwrap();
//         assert_eq!(merged_task_set.tasks.len(), 1);
//         assert_eq!(merged_task_set.tasks[0].id, "task1");
//         assert_eq!(merged_task_set.hlc.timestamp, 101);
//     }
// }
