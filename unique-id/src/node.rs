use anyhow::Context;
use serde::{Deserialize, Serialize};

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
    Generate {
        msg_id: u32,
    },
    GenerateOk {
        in_reply_to: u32,
        id: String,
    },
}

#[derive(Debug, Default)]
pub struct NodeState {
    last_message_id: u32,
}

#[derive(Debug)]
pub struct Node<'a> {
    pub node_id: String,
    pub state: &'a mut NodeState,
}

impl<'a> Node<'a> {
    pub fn init(line: String, state: &'a mut NodeState) -> anyhow::Result<Self> {
        let msg: Message = serde_json::from_str(&line).context("Message deserialization error")?;

        match msg.body.clone() {
            MessageBody::Init {
                msg_id,
                node_id,
                node_ids: _,
            } => {
                let node = Self {
                    node_id: node_id.clone(),
                    state,
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
        let body: Option<MessageBody> = match req.body.clone() {
            MessageBody::Generate { msg_id } => {
                self.state.last_message_id += 1;

                Some(MessageBody::GenerateOk {
                    in_reply_to: msg_id,
                    id: format!("{}:{}", self.node_id, self.state.last_message_id),
                })
            }
            body => unimplemented!("Message {:?} not implemented", body),
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
