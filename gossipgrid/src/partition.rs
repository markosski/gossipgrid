use bincode::{Decode, Encode};
use log::{error, info};
use rand::RngCore;
use std::collections::BTreeMap;
use std::collections::HashMap;

use crate::node::NodeId;

pub type VNode = u16;

#[derive(Debug, Clone, Encode, Decode, PartialEq)]
pub struct PartitionMap {
    vnode_replicas: HashMap<VNode, Vec<NodeId>>, // RF > 1
    vnode_count: u16,
    replication_factor: u8,
}

pub fn hash_combined(vnode: VNode, node: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    use twox_hash::XxHash64;

    let mut h = XxHash64::with_seed(0);
    (vnode, node).hash(&mut h);
    h.finish()
}

impl PartitionMap {
    pub fn new(vnode_count: &u16, replication_factor: &u8) -> Self {
        Self {
            vnode_replicas: HashMap::new(),
            vnode_count: vnode_count.clone(),
            replication_factor: replication_factor.clone(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.vnode_replicas.is_empty()
    }

    pub fn empty() -> Self {
        Self {
            vnode_replicas: HashMap::new(),
            vnode_count: 0,
            replication_factor: 0,
        }
    }

    pub fn assign(&mut self, nodes: &[NodeId]) {
        self.vnode_replicas.clear();

        for vnode in 0..self.vnode_count {
            let mut hashed: BTreeMap<u64, &NodeId> = BTreeMap::new();

            for node in nodes {
                let h = hash_combined(vnode, node);
                hashed.insert(h, node);
            }

            let owners = hashed
                .values()
                .take(self.replication_factor as usize)
                .cloned()
                .cloned()
                .collect::<Vec<_>>();

            self.vnode_replicas.insert(vnode, owners);
        }
    }

    pub fn hash_key(&self, key: &str) -> VNode {
        use std::hash::{Hash, Hasher};
        use twox_hash::XxHash64;

        let mut h = XxHash64::with_seed(0);
        key.hash(&mut h);
        (h.finish() % self.vnode_count as u64) as VNode
    }

    pub fn route(&self, this_node: &NodeId, key: &str) -> Option<NodeId> {
        let vnode = self.hash_key(key);
        self.vnode_replicas.get(&vnode).and_then(|replicas| {
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

    pub fn contains_vnode(&self, node: &NodeId, vnode: &VNode) -> bool {
        match self.vnode_replicas.get(vnode) {
            Some(replicas) => replicas.contains(node),
            None => {
                error!("VNode {} not found in partition map", vnode);
                false
            }
        }
    }

    pub fn get_replicas(&self, key: &str) -> Option<&Vec<NodeId>> {
        let vnode = self.hash_key(key);
        self.vnode_replicas.get(&vnode)
    }

    pub fn get_vnodes_for_node(&self, node: &NodeId) -> Vec<VNode> {
        let mut nodes = Vec::new();
        for (vnode, replicas) in &self.vnode_replicas {
            if replicas.contains(node) {
                nodes.push(*vnode);
            }
        }
        nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
