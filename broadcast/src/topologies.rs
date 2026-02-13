use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum Topology {
    StarTopology,
    FullMeshTopology,
    RingTopology,
}

impl Topology {
    pub fn get_topology(self, node_ids: &Vec<String>) -> HashMap<String, Vec<String>> {
        let mut topology: HashMap<String, Vec<String>> = HashMap::new();

        match self {
            Topology::StarTopology => {
                let mut node_ids_iter = node_ids.iter();

                let star_node = node_ids_iter.next().unwrap();
                let mut followers: Vec<String> = Vec::new();

                for node_id in node_ids_iter {
                    followers.push(node_id.clone());
                    topology.insert(node_id.clone(), vec![star_node.clone()]);
                }

                topology.insert(star_node.clone(), followers);

                topology
            }
            Topology::FullMeshTopology => {
                for node_id in node_ids.iter() {
                    topology.insert(
                        node_id.clone(),
                        node_ids
                            .iter()
                            .filter(|id| *id != node_id)
                            .cloned()
                            .collect(),
                    );
                }

                topology
            }
            Topology::RingTopology => {
                let node_ids_size = node_ids.len();

                for (index, node_id) in node_ids.iter().enumerate() {
                    let follower = if index + 1 < node_ids_size {
                        node_ids[index + 1].clone()
                    } else {
                        node_ids[0].clone()
                    };

                    topology.insert(node_id.clone(), vec![follower]);
                }

                topology
            }
        }
    }
}
