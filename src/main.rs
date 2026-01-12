use std::io::{self};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Default)]
struct NodeState {
    node_id: String,
    last_message_id: u32,
}

#[derive(Debug)]
struct Node<'a> {
    state: &'a mut NodeState,
}

impl<'a> Node<'a> {
    fn new(state: &'a mut NodeState) -> Self {
        Self { state }
    }

    fn reply(&mut self, message: Message) -> Message {
        match message.body {
            MessageBody::Init {
                msg_id,
                node_id,
                node_ids: _,
            } => {
                self.state.node_id = node_id;

                Message {
                    src: self.state.node_id.clone(),
                    dest: message.src,
                    body: MessageBody::InitOk {
                        in_reply_to: msg_id,
                    },
                }
            }
            MessageBody::Echo { msg_id, echo } => {
                self.state.last_message_id += 1;

                Message {
                    src: self.state.node_id.clone(),
                    dest: message.src,
                    body: MessageBody::EchoOk {
                        msg_id: self.state.last_message_id,
                        in_reply_to: msg_id,
                        echo,
                    },
                }
            }
            MessageBody::Generate { msg_id } => {
                self.state.last_message_id += 1;

                Message {
                    src: self.state.node_id.clone(),
                    dest: message.src,
                    body: MessageBody::GenerateOk {
                        in_reply_to: msg_id,
                        id: Uuid::new_v4(),
                    },
                }
            }
            _ => unimplemented!("Message reply not implemented yet"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    src: String,
    dest: String,
    body: MessageBody,
}

#[derive(Debug, Serialize, Deserialize)]
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
        in_reply_to: u32,
        id: Uuid,
    },
}

fn main() -> anyhow::Result<()> {
    let mut state = NodeState::default();
    let mut node = Node::new(&mut state);

    for line in io::stdin().lines() {
        let content = line?;

        let message: Message =
            serde_json::from_str(&content).context("Message deserialization error")?;

        let response = node.reply(message);

        let json = serde_json::to_string(&response).context("Message serialization error")?;

        println!("{}", json);
    }

    Ok(())
}
