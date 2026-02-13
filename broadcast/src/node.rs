use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use crate::topologies::Topology;

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
    Broadcast {
        msg_id: u32,
        message: i32,
    },
    BroadcastOk {
        in_reply_to: u32,
    },
    Read {
        msg_id: u32,
    },
    ReadOk {
        messages: HashSet<i32>,
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
        messages: HashSet<i32>,
    },
    GossipOk {
        in_reply_to: u32,
        messages: HashSet<i32>,
    },
}

#[derive(Debug, Default, Clone)]
pub struct NodeState {
    pub messages: HashSet<i32>,
    /// It represents a vector of node ids. These nodes will be used for gossiping.
    pub neighbors: Vec<String>,
    pub last_message_id: u32,
    pub pending_to_send: HashSet<i32>,
}

#[derive(Debug)]
pub struct Node {
    pub node_id: String,
    pub node_ids: Vec<String>,
    pub state: Arc<Mutex<NodeState>>,
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
                    node_ids,
                    state: state.clone(),
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

    pub fn handle(&mut self, req: Message) -> anyhow::Result<Option<Message>> {
        let topology = Topology::StarTopology.get_topology(&self.node_ids);

        let body: Option<MessageBody> = match req.body.clone() {
            MessageBody::Broadcast { msg_id, message } => {
                let mut state = self
                    .state
                    .lock()
                    .expect("State poisoned when replying to a broadcast message");

                state.messages.insert(message);
                state.pending_to_send.insert(message);

                Some(MessageBody::BroadcastOk {
                    in_reply_to: msg_id,
                })
            }
            MessageBody::Read { msg_id } => {
                let state = self
                    .state
                    .lock()
                    .expect("State poisoned when replying to a broadcast message");

                Some(MessageBody::ReadOk {
                    messages: state.messages.clone(),
                    in_reply_to: msg_id,
                })
            }
            MessageBody::Topology {
                msg_id,
                topology: _,
            } => {
                let mut state = self
                    .state
                    .lock()
                    .expect("State poisoned when replying to a broadcast message");

                state.neighbors = topology
                    .get(&self.node_id)
                    .with_context(|| format!("Node {} does not have neighbors", self.node_id))?
                    .to_vec();

                Some(MessageBody::TopologyOk {
                    in_reply_to: msg_id,
                })
            }
            MessageBody::Gossip {
                msg_id,
                messages: external_messages,
            } => {
                let mut state = self
                    .state
                    .lock()
                    .expect("State poisoned when replying to a broadcast message");

                let internal_messages = state.messages.clone();
                let new_messages: HashSet<i32> = internal_messages
                    .clone()
                    .into_iter()
                    .filter(|m| !external_messages.contains(m))
                    .collect();

                // It adds to pending_gossips only the messages that the current node does not have
                state.pending_to_send.extend(new_messages.clone());
                state.messages.extend(external_messages);

                Some(MessageBody::GossipOk {
                    // We send all our messages back as part of the gossip ok response because the
                    // node that sent us that message can ensure if the message was successfully
                    // sent but comparing what we have with what they.
                    messages: state.messages.clone(),
                    in_reply_to: msg_id,
                })
            }
            MessageBody::GossipOk {
                in_reply_to: _,
                messages: external_messages,
            } => {
                let mut state = self
                    .state
                    .lock()
                    .expect("State poisoned when replying to a broadcast message");

                let internal_messages = state.messages.clone();
                // Check if there are messages missing from the node sending the gossip ok message.
                // We just compare the current node messages (which is the one that send the
                // gossip) with the messages arriving from the destination node.
                //
                // This solution assumes that gossip ok includes all messages from the destination
                // node.
                let lost_messages = internal_messages
                    .clone()
                    .into_iter()
                    .filter(|m| !external_messages.contains(m));

                state.pending_to_send.extend(lost_messages);
                state.messages.extend(external_messages);

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
