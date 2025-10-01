use bincode::{Decode, Encode};
use log::{error};
use rand::RngCore;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;

use crate::node::NodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode, Serialize, Deserialize)]
pub struct PartitionId(pub u16);
impl PartitionId {
    pub fn value(self) -> u16 {
        self.0
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u16> for PartitionId {
    fn from(value: u16) -> Self {
        PartitionId(value)
    }
}

#[derive(Debug, Clone, Encode, Decode, PartialEq)]
pub struct PartitionMap {
    partition_replicas: HashMap<PartitionId, Vec<NodeId>>, // RF > 1
    partition_count: u16,
    replication_factor: u8,
}

pub fn hash_combined(partition: PartitionId, node: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    use twox_hash::XxHash64;

    let mut h = XxHash64::with_seed(0);
    (partition, node).hash(&mut h);
    h.finish()
}

impl PartitionMap {
    pub fn new(partition_count: &u16, replication_factor: &u8) -> Self {
        Self {
            partition_replicas: HashMap::new(),
            partition_count: partition_count.clone(),
            replication_factor: replication_factor.clone(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.partition_replicas.is_empty()
    }

    pub fn empty() -> Self {
        Self {
            partition_replicas: HashMap::new(),
            partition_count: 0,
            replication_factor: 0,
        }
    }

    pub fn assign(&mut self, nodes: &[NodeId]) {
        self.partition_replicas.clear();

        for partition_id in 0..self.partition_count {
    
            let mut hashed: BTreeMap<u64, &NodeId> = BTreeMap::new();

            for node in nodes {
                let h = hash_combined(partition_id.into(), node);
                hashed.insert(h, node);
            }

            let owners = hashed
                .values()
                .take(self.replication_factor as usize)
                .cloned()
                .cloned()
                .collect::<Vec<_>>();

            self.partition_replicas.insert(partition_id.into(), owners);
        }
    }

    pub fn hash_key(&self, key: &str) -> PartitionId {
        use std::hash::{Hash, Hasher};
        use twox_hash::XxHash64;

        let mut h = XxHash64::with_seed(0);
        key.hash(&mut h);
        let id = h.finish() % self.partition_count as u64;
        PartitionId(id as u16)
    }

    /// Route the key to one of the replicas, preferring this_node if it is one of the replicas
    pub fn route(&self, this_node: &NodeId, key: &str) -> Option<NodeId> {
        let partition = self.hash_key(key);
        self.partition_replicas.get(&partition).and_then(|replicas| {
            // if this_node is one of the replicas, return it
            if replicas.contains(this_node) {
                Some(this_node.clone())
            } else {
                let mut random = rand::rng();
                replicas
                    .get(random.next_u32() as usize % replicas.len())
                    .cloned()
            }
        })
    }

    /// Check if the given partition is assigned to the given NodeId
    pub fn contains_partition(&self, node: &NodeId, partition: &PartitionId) -> bool {
        match self.partition_replicas.get(partition) {
            Some(replicas) => replicas.contains(node),
            None => {
                error!("Partition {} not found in partition map", partition);
                false
            }
        }
    }

    pub fn get_replicas(&self, key: &str) -> Option<&Vec<NodeId>> {
        let partition = self.hash_key(key);
        self.partition_replicas.get(&partition)
    }

    pub fn get_partitions_for_node(&self, node: &NodeId) -> Vec<PartitionId> {
        let mut nodes = Vec::new();
        for (partition, replicas) in &self.partition_replicas {
            if replicas.contains(node) {
                nodes.push(*partition);
            }
        }
        nodes
    }
}

#[cfg(test)]
mod tests {
    // use super::*;

    // #[test]
    // fn test_partition_map() {
    //     env_logger::init();
    //     let mut partition_map = PartitionMap::new(&10, &2);
    //     let nodes = vec![
    //         "node1".to_string(),
    //         "node2".to_string(),
    //         "node3".to_string(),
    //     ];
    //     partition_map.assign(&nodes);

    //     assert_eq!(partition_map.vnode_count, 10);
    //     assert_eq!(partition_map.replication_factor, 2);

    //     for vnode in 0..10 {
    //         let owner = partition_map.vnode_to_node.get(&vnode).unwrap();
    //         assert!(nodes.contains(owner));
    //         let replicas = partition_map.vnode_replicas.get(&vnode).unwrap();
    //         assert_eq!(replicas.len(), 2);
    //         for replica in replicas {
    //             assert!(nodes.contains(replica));
    //             println!("replicas: {} {:?}", &vnode, replica);
    //         }
    //     }
    // }
}
