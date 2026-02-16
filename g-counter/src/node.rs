use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::topologies::Topology;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CounterValue {
    msg_id: u32,
    value: i32,
}

/// It stores counter values from different nodes in order to act as a global counter.
///
/// Whenever a client wants to read the counter value, we sum all values from the different
/// node counters.
///
/// The key of the hashmap represents the node id.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Counter {
    data: HashMap<String, CounterValue>,
}

impl Counter {
    /// Adds a value to the node counter
    fn add(&mut self, node_id: &str, new_counter: CounterValue) {
        match self.data.get_mut(node_id) {
            Some(counter) => {
                counter.value += new_counter.value;
                counter.msg_id = new_counter.msg_id;
            }
            None => {
                self.data.insert(node_id.to_string(), new_counter);
            }
        }
    }

    /// It merge the value from another node counter version by taking the lastest one.
    ///
    /// As msg_id property is an incremental unique value per node, we just check if the msg_id
    /// from the new counter value is higher than the current one.  
    ///
    /// In case we do not have any counter for a node, we just insert it.
    ///
    /// This function is used when values are coming from a gossip, and it ensures that
    /// the node always has the lastest values.
    fn merge(&mut self, node_id: &str, new_counter: CounterValue) {
        match self.data.get_mut(node_id) {
            Some(counter) => {
                if counter.msg_id < new_counter.msg_id {
                    counter.value = new_counter.value;
                    counter.msg_id = new_counter.msg_id;
                }
            }
            None => {
                self.data.insert(node_id.to_string(), new_counter);
            }
        }
    }

    /// Sums all the nodes counters values, which gives the feeling of a unique global counter.
    fn sum(&self) -> i32 {
        let mut total = 0;

        for (_, counter) in self.data.iter() {
            total += counter.value;
        }

        total
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: MessageBody,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageBody {
    Init {
        msg_id: u32,
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {
        in_reply_to: u32,
    },
    Read {
        msg_id: u32,
    },
    ReadOk {
        value: i32,
        in_reply_to: u32,
    },
    Topology {
        msg_id: u32,
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk {
        in_reply_to: u32,
    },
    Gossip {
        msg_id: u32,
        counter: Counter,
    },
    GossipOk {
        counter: Counter,
        in_reply_to: u32,
    },
    Add {
        msg_id: u32,
        delta: i32,
    },
    AddOk {
        in_reply_to: u32,
    },
}

#[derive(Debug)]
pub enum Event {
    // A node replies the request of a client.
    Reply(Message),
    // A node actively sends a message to a node or multiple nodes. An example
    // of this event would be sending a gossip message to node's neighbors.
    Push(Message),
    // A node should shutdown
    Shutdown,
}

#[derive(Debug, Default)]
pub struct NodeState {
    pub last_message_id: u32,
    pub counter: Counter,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub node_id: String,
    pub state: Arc<Mutex<NodeState>>,
    topology: HashMap<String, Vec<String>>,
}

impl Node {
    pub fn init(line: String, state: Arc<Mutex<NodeState>>) -> anyhow::Result<Self> {
        let msg: Message = serde_json::from_str(&line).context("Message deserialization error")?;

        match msg.body.clone() {
            MessageBody::Init {
                msg_id,
                node_id,
                node_ids,
            } => {
                let node = Self {
                    node_id: node_id.clone(),
                    state: state.clone(),
                    topology: Topology::RingTopology.get_topology(&node_ids),
                };

                let reply = Message {
                    src: node_id,
                    dest: msg.src,
                    body: MessageBody::InitOk {
                        in_reply_to: msg_id,
                    },
                };

                node.write(reply)?;

                Ok(node)
            }
            _ => Err(anyhow::anyhow!(
                "Init message is not the first message received"
            )),
        }
    }

    pub fn get_neighbors(&self) -> anyhow::Result<Vec<String>> {
        self.topology
            .get(&self.node_id)
            .with_context(|| format!("Node {} does not have neighbors", self.node_id))
            .cloned()
    }

    pub fn handle(&mut self, req: Message) -> anyhow::Result<Option<Message>> {
        let body: Option<MessageBody> = match req.body.clone() {
            MessageBody::Read { msg_id } => {
                let state = self
                    .state
                    .lock()
                    .expect("State poisoned when replying to a broadcast message");

                Some(MessageBody::ReadOk {
                    value: state.counter.sum(),
                    in_reply_to: msg_id,
                })
            }
            MessageBody::Add { msg_id, delta } => {
                let mut state = self
                    .state
                    .lock()
                    .expect("State poisoned when replying to a broadcast message");

                let last_msg_id = state.last_message_id + 1;

                state.last_message_id = last_msg_id;
                state.counter.add(
                    &self.node_id,
                    CounterValue {
                        msg_id: last_msg_id,
                        value: delta,
                    },
                );

                Some(MessageBody::AddOk {
                    in_reply_to: msg_id,
                })
            }
            MessageBody::Gossip { msg_id, counter } => {
                let mut state = self
                    .state
                    .lock()
                    .expect("State poisoned when replying to a Gossip message");

                // Merge all node's values the comming data except by itself.
                for (node_id, c) in counter
                    .data
                    .iter()
                    .filter(|(node_id, _)| **node_id != self.node_id)
                {
                    state.counter.merge(node_id, c.clone());
                }

                Some(MessageBody::GossipOk {
                    counter: state.counter.clone(),
                    in_reply_to: msg_id,
                })
            }
            MessageBody::GossipOk {
                in_reply_to: _,
                counter,
            } => {
                let mut state = self
                    .state
                    .lock()
                    .expect("State poisoned when replying to a Gossip message");

                // Merge all node's values with the coming except by itself.
                for (node_id, c) in counter
                    .data
                    .iter()
                    .filter(|(node_id, _)| **node_id != self.node_id)
                {
                    state.counter.merge(node_id, c.clone());
                }

                None
            }
            body => unimplemented!("Message {:?} not implemented yet", body),
        };

        match body {
            Some(b) => Ok(Some(Message {
                src: self.node_id.clone(),
                dest: req.src.clone(),
                body: b,
            })),
            None => Ok(None),
        }
    }

    pub fn write(&self, msg: Message) -> anyhow::Result<()> {
        let json = serde_json::to_string(&msg).context("Message serialization error")?;

        println!("{}", json);

        Ok(())
    }
}
