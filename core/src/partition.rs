use bincode::{Decode, Encode};
use log::{error, info};
use std::collections::BTreeMap;
use std::collections::HashMap;

type NodeId = String;
type VNode = usize;
type Key = String;

#[derive(Debug, Clone, Encode, Decode)]
pub struct PartitionMap {
    vnode_to_node: HashMap<VNode, NodeId>,       // primary owner
    vnode_replicas: HashMap<VNode, Vec<NodeId>>, // RF > 1
    vnode_count: usize,
    replication_factor: usize,
}

impl PartitionMap {
    pub fn new(vnode_count: &usize, replication_factor: &usize) -> Self {
        Self {
            vnode_to_node: HashMap::new(),
            vnode_replicas: HashMap::new(),
            vnode_count: vnode_count.clone(),
            replication_factor: replication_factor.clone(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.vnode_to_node.is_empty() && self.vnode_replicas.is_empty()
    }

    pub fn empty() -> Self {
        Self {
            vnode_to_node: HashMap::new(),
            vnode_replicas: HashMap::new(),
            vnode_count: 0,
            replication_factor: 0,
        }
    }

    pub fn assign(&mut self, nodes: &[NodeId]) {
        self.vnode_to_node.clear();
        self.vnode_replicas.clear();

        for vnode in 0..self.vnode_count {
            let mut hashed: BTreeMap<u64, &NodeId> = BTreeMap::new();

            for node in nodes {
                let h = Self::hash_combined(vnode, node);
                hashed.insert(h, node);
            }

            let owners = hashed
                .values()
                .take(self.replication_factor)
                .cloned()
                .cloned()
                .collect::<Vec<_>>();

            self.vnode_to_node.insert(vnode, owners[0].clone());
            self.vnode_replicas.insert(vnode, owners);
        }
    }

    fn hash_combined(vnode: VNode, node: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        use twox_hash::XxHash64;

        let mut h = XxHash64::with_seed(0);
        (vnode, node).hash(&mut h);
        h.finish()
    }

    fn hash_key(&self, key: &str) -> VNode {
        use std::hash::{Hash, Hasher};
        use twox_hash::XxHash64;

        let mut h = XxHash64::with_seed(0);
        key.hash(&mut h);
        (h.finish() % self.vnode_count as u64) as VNode
    }

    fn route(&self, key: &str) -> Option<&NodeId> {
        let vnode = self.hash_key(key);
        self.vnode_to_node.get(&vnode)
    }

    fn get_replicas(&self, key: &str) -> Option<&Vec<NodeId>> {
        let vnode = self.hash_key(key);
        self.vnode_replicas.get(&vnode)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_map() {
        env_logger::init();
        let mut partition_map = PartitionMap::new(&10, &3);
        let nodes = vec![
            "node1".to_string(),
            "node2".to_string(),
            "node3".to_string(),
        ];
        partition_map.assign(&nodes);

        assert_eq!(partition_map.vnode_count, 10);
        assert_eq!(partition_map.replication_factor, 3);

        for vnode in 0..10 {
            let owner = partition_map.vnode_to_node.get(&vnode).unwrap();
            assert!(nodes.contains(owner));
            let replicas = partition_map.vnode_replicas.get(&vnode).unwrap();
            assert_eq!(replicas.len(), 3);
            for replica in replicas {
                assert!(nodes.contains(replica));
                println!("replicas: {} {:?}", &vnode, replica);
            }
        }
    }
}
