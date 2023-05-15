mod node;

pub(crate) use node::Node;

fn default_md5_hash_fn(input: &[u8]) -> Vec<u8> {
    use md5::{Digest, Md5};

    // create a Md5 hasher instance
    let mut hasher = Md5::new();

    // process input message
    hasher.update(input);

    // acquire hash digest in the form of GenericArray,
    // which in this case is equivalent to [u8; 16]
    hasher.finalize().to_vec()
}

/// Consistent Hash
pub struct ConsistentHash<N: Node> {
    hash_fn: fn(&[u8]) -> Vec<u8>,
    nodes: std::collections::BTreeMap<Vec<u8>, N>,
    replicas: std::collections::HashMap<String, usize>,
}

impl<N: Node> ConsistentHash<N> {
    /// Construct with default hash function (Md5)
    pub fn new() -> ConsistentHash<N> {
        ConsistentHash::with_hash(default_md5_hash_fn)
    }

    /// Construct with customized hash function
    pub fn with_hash(hash_fn: fn(&[u8]) -> Vec<u8>) -> ConsistentHash<N> {
        use std::collections::{BTreeMap, HashMap};

        ConsistentHash {
            hash_fn,
            nodes: BTreeMap::new(),
            replicas: HashMap::new(),
        }
    }

    /// Add a new node
    pub fn add(&mut self, node: &N, num_replicas: usize) {
        let node_name = node.name();

        // Remove it first
        self.remove(node);

        self.replicas.insert(node_name.clone(), num_replicas);
        for replica in 0..num_replicas {
            let node_ident = format!("{}:{}", node_name, replica);
            let key = (self.hash_fn)(node_ident.as_bytes());

            self.nodes.insert(key, node.clone());
        }
    }

    /// Get a node by key. Return `None` if no valid node inside
    pub fn get<'a>(&'a self, key: &[u8]) -> Option<&'a N> {
        if self.nodes.is_empty() {
            return None;
        }

        let hashed_key = (self.hash_fn)(key);

        let entry = self.nodes.range(hashed_key..).next();
        if let Some((_k, v)) = entry {
            return Some(v);
        }

        // Back to the first one
        let first = self.nodes.iter().next();
        debug_assert!(first.is_some());
        let (_k, v) = first.unwrap();
        Some(v)
    }

    #[allow(dead_code)]
    /// Get a node by string key
    pub fn get_str<'a>(&'a self, key: &str) -> Option<&'a N> {
        self.get(key.as_bytes())
    }

    #[allow(dead_code)]
    /// Get a node by key. Return `None` if no valid node inside
    pub fn get_mut<'a>(&'a mut self, key: &[u8]) -> Option<&'a mut N> {
        let hashed_key = self.get_node_hashed_key(key);
        hashed_key.and_then(move |k| self.nodes.get_mut(&k))
    }

    #[allow(dead_code)]
    // Get a node's hashed key by key. Return `None` if no valid node inside
    fn get_node_hashed_key(&self, key: &[u8]) -> Option<Vec<u8>> {
        if self.nodes.is_empty() {
            return None;
        }

        let hashed_key = (self.hash_fn)(key);

        let entry = self.nodes.range(hashed_key..).next();
        if let Some((k, _v)) = entry {
            return Some(k.clone());
        }

        // Back to the first one
        let first = self.nodes.iter().next();
        debug_assert!(first.is_some());
        let (k, _v) = first.unwrap();
        Some(k.clone())
    }

    #[allow(dead_code)]
    /// Get a node by string key
    pub fn get_str_mut<'a>(&'a mut self, key: &str) -> Option<&'a mut N> {
        self.get_mut(key.as_bytes())
    }

    /// Remove a node with all replicas (virtual nodes)
    pub fn remove(&mut self, node: &N) {
        let node_name = node.name();

        let num_replicas = match self.replicas.remove(&node_name) {
            Some(val) => val,
            None => {
                return;
            }
        };

        for replica in 0..num_replicas {
            let node_ident = format!("{}:{}", node.name(), replica);
            let key = (self.hash_fn)(node_ident.as_bytes());
            self.nodes.remove(&key);
        }
    }

    #[allow(dead_code)]
    /// Number of nodes
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    #[allow(dead_code)]
    /// Is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<N: Node> Default for ConsistentHash<N> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Debug, Clone, Eq, PartialEq)]
    struct ServerNode {
        host: String,
        port: u16,
    }

    impl Node for ServerNode {
        fn name(&self) -> String {
            format!("{}:{}", self.host, self.port)
        }
    }

    impl ServerNode {
        fn new(host: &str, port: u16) -> ServerNode {
            ServerNode {
                host: host.to_owned(),
                port,
            }
        }
    }

    #[test]
    fn test_basic() {
        let nodes = [
            ServerNode::new("localhost", 12345),
            ServerNode::new("localhost", 12346),
            ServerNode::new("localhost", 12347),
            ServerNode::new("localhost", 12348),
            ServerNode::new("localhost", 12349),
            ServerNode::new("localhost", 12350),
            ServerNode::new("localhost", 12351),
            ServerNode::new("localhost", 12352),
            ServerNode::new("localhost", 12353),
        ];

        const REPLICAS: usize = 20;

        let mut ch = ConsistentHash::new();

        for node in nodes.iter() {
            ch.add(node, REPLICAS);
        }

        assert_eq!(ch.len(), nodes.len() * REPLICAS);

        let node_for_hello = ch.get_str("hello").unwrap().clone();
        assert_eq!(node_for_hello, ServerNode::new("localhost", 12347));

        ch.remove(&ServerNode::new("localhost", 12350));
        assert_eq!(ch.get_str("hello").unwrap().clone(), node_for_hello);

        assert_eq!(ch.len(), (nodes.len() - 1) * REPLICAS);

        ch.remove(&ServerNode::new("localhost", 12347));
        assert_ne!(ch.get_str("hello").unwrap().clone(), node_for_hello);

        assert_eq!(ch.len(), (nodes.len() - 2) * REPLICAS);
    }

    #[test]
    fn get_from_empty() {
        let mut ch = ConsistentHash::<ServerNode>::new();
        assert_eq!(ch.get_str(""), None);
        assert_eq!(ch.get_str_mut(""), None);
    }

    #[test]
    fn get_from_one_node() {
        let mut node = ServerNode::new("localhost", 12345);
        for replicas in 1..10_usize {
            let mut ch = ConsistentHash::<ServerNode>::new();
            ch.add(&node, replicas);
            assert_eq!(ch.len(), replicas);
            for i in 0..replicas * 100 {
                let s = format!("{}", i);
                assert_eq!(ch.get_str(&s), Some(&node));
                assert_eq!(ch.get_str_mut(&s), Some(&mut node));
            }
        }
    }

    #[test]
    fn get_from_two_nodes() {
        let mut node0 = ServerNode::new("localhost", 12345);
        let mut node1 = ServerNode::new("localhost", 54321);
        for replicas in 1..10_usize {
            let mut ch = ConsistentHash::<ServerNode>::new();
            ch.add(&node0, replicas);
            ch.add(&node1, replicas);
            assert_eq!(ch.len(), 2 * replicas);
            for i in 0..replicas * 100 {
                let s = format!("{}", i);
                let n = ch.get_str(&s).unwrap();
                assert!(n == &node0 || n == &node1);
                let n = ch.get_str(&s).unwrap();
                assert!(n == &mut node0 || n == &mut node1);
            }
        }
    }

    #[test]
    fn get_exact_node() {
        let mut ch = ConsistentHash::new();
        const NODES: usize = 1000;
        const REPLICAS: usize = 20;
        let mut nodes = Vec::<ServerNode>::with_capacity(NODES);
        for i in 0..NODES {
            let node = ServerNode::new("localhost", 10000 + i as u16);
            ch.add(&node, REPLICAS);
            nodes.push(node);
        }
        assert_eq!(ch.len(), NODES * REPLICAS);
        for i in 0..NODES {
            for r in 0..REPLICAS {
                let s = format!("{}:{}", nodes[i].name(), r);
                assert_eq!(ch.get_str(&s), Some(&nodes[i]));
                assert_eq!(ch.get_str_mut(&s).cloned().as_ref(), Some(&nodes[i]));
            }
        }
    }
}
