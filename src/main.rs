use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::{self},
    sync::{Arc, Mutex},
    thread,
};
use uuid::Uuid;

#[derive(Debug, Default)]
struct NodeState {
    node_id: String,
    messages: HashSet<i32>,
    /// It represents a vector of node ids. These nodes will be used for gossiping.
    neighbors: Vec<String>,
    last_message_id: u32,
}

#[derive(Debug)]
struct Node {
    state: Arc<Mutex<NodeState>>,
}

impl<'a> Node {
    fn new(state: Arc<Mutex<NodeState>>) -> Self {
        Self { state }
    }

    fn reply(&mut self, req: Message) -> anyhow::Result<Option<Message>> {
        let mut state = self
            .state
            .lock()
            .expect("State poisoned while building reply");

        let body: Option<MessageBody> = match req.body.clone() {
            MessageBody::Init {
                msg_id,
                node_id,
                node_ids: _,
            } => {
                state.node_id = node_id;

                Some(MessageBody::InitOk {
                    in_reply_to: msg_id,
                })
            }
            MessageBody::Echo { msg_id, echo } => Some(MessageBody::EchoOk {
                msg_id: state.last_message_id + 1,
                in_reply_to: msg_id,
                echo,
            }),
            MessageBody::Generate { msg_id } => Some(MessageBody::GenerateOk {
                msg_id: state.last_message_id + 1,
                in_reply_to: msg_id,
                id: Uuid::new_v4(),
            }),
            MessageBody::Broadcast { msg_id, message } => {
                state.messages.insert(message);

                if let Some(in_reply_to) = msg_id {
                    Some(MessageBody::BroadcastOk {
                        in_reply_to,
                        msg_id: state.last_message_id + 1,
                    })
                } else {
                    None
                }
            }
            MessageBody::Read { msg_id } => Some(MessageBody::ReadOk {
                msg_id: state.last_message_id + 1,
                messages: state.messages.clone(),
                in_reply_to: msg_id,
            }),
            MessageBody::Topology { msg_id, topology } => {
                state.neighbors = topology.keys().map(|k| k.to_string()).collect();

                Some(MessageBody::TopologyOk {
                    msg_id: state.last_message_id + 1,
                    in_reply_to: msg_id,
                })
            }
            body => unimplemented!("Message {:?} not implemented yet", body),
        };

        Ok(self.build_reply(&req, &mut state, body))
    }

    fn build_reply(
        &self,
        req: &Message,
        state: &mut NodeState,
        body: Option<MessageBody>,
    ) -> Option<Message> {
        state.last_message_id += 1;

        Some(Message {
            src: state.node_id.clone(),
            dest: req.src.clone(),
            body: body?,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Message {
    src: String,
    dest: String,
    body: MessageBody,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MessageBody {
    Echo {
        msg_id: u32,
        echo: String,
    },
    EchoOk {
        msg_id: u32,
        in_reply_to: u32,
        echo: String,
    },
    Init {
        msg_id: u32,
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {
        in_reply_to: u32,
    },
    Generate {
        msg_id: u32,
    },
    GenerateOk {
        msg_id: u32,
        in_reply_to: u32,
        id: Uuid,
    },
    Broadcast {
        msg_id: Option<u32>,
        message: i32,
    },
    BroadcastOk {
        msg_id: u32,
        in_reply_to: u32,
    },
    Read {
        msg_id: u32,
    },
    ReadOk {
        msg_id: u32,
        messages: HashSet<i32>,
        in_reply_to: u32,
    },
    Topology {
        msg_id: u32,
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk {
        msg_id: u32,
        in_reply_to: u32,
    },
}

fn main() -> anyhow::Result<()> {
    let state = Arc::new(Mutex::new(NodeState::default()));
    let mut node = Node::new(state.clone());

    for line in io::stdin().lines() {
        let content = line?;

        let msg: Message =
            serde_json::from_str(&content).context("Message deserialization error")?;

        if let Some(response) = node.reply(msg.clone())? {
            let json = serde_json::to_string(&response).context("Message serialization error")?;

            println!("{}", json);

            // Gossiping messages to neighbour
            match msg.body {
                MessageBody::Broadcast { msg_id: _, message } => {
                    let state = node.state.lock().expect("State poisoned");
                    let neighbors = state.neighbors.clone();

                    for neighbor in neighbors {
                        let node_id = state.node_id.clone();

                        thread::spawn(move || -> anyhow::Result<()> {
                            let message = Message {
                                src: node_id,
                                dest: neighbor,
                                body: MessageBody::Broadcast {
                                    msg_id: None,
                                    message,
                                },
                            };

                            let json = serde_json::to_string(&message)
                                .context("Message serialization error")?;

                            println!("{}", json);

                            Ok(())
                        });
                    }
                }
                _ => (),
            };
        }
    }

    Ok(())
}
