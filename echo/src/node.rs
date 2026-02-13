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
    Echo {
        msg_id: u32,
        echo: String,
    },
    EchoOk {
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
}

#[derive(Debug)]
pub struct Node {
    pub node_id: String,
}

impl Node {
    pub fn init(line: String) -> anyhow::Result<Self> {
        let msg: Message = serde_json::from_str(&line).context("Message deserialization error")?;

        match msg.body.clone() {
            MessageBody::Init {
                msg_id,
                node_id,
                node_ids: _,
            } => {
                let node = Self {
                    node_id: node_id.clone(),
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
            MessageBody::Echo { msg_id, echo } => Some(MessageBody::EchoOk {
                in_reply_to: msg_id,
                echo,
            }),
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
