use anyhow::Context;
use core::time;
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
    node_ids: Vec<String>,
    messages: HashSet<i32>,
    /// It represents a vector of node ids. These nodes will be used for gossiping.
    neighbors: Vec<String>,
    last_message_id: u32,
    gossip_messages: GossipMessages,
}

#[derive(Debug, Default)]
struct GossipMessages {
    pending: HashSet<i32>,
    sent: HashMap<u32, Message>,
}

impl GossipMessages {
    fn insert_pending(&mut self, value: i32) {
        self.pending.insert(value);
    }

    fn batch_insert_pending<'a, T: IntoIterator<Item = &'a i32>>(&mut self, values: T) {
        self.pending.extend(values);
    }

    fn clear_pending(&mut self) {
        self.pending.clear();
    }

    fn insert_sent(&mut self, msg_id: u32, message: &Message) {
        self.sent.insert(msg_id, message.clone());
    }

    fn remove_message_sent(&mut self, msg_id: u32) {
        self.sent.remove(&msg_id);
    }
}

#[derive(Debug, Clone)]
struct Node {
    state: Arc<Mutex<NodeState>>,
}

impl<'a> Node {
    fn init(line: String, state: Arc<Mutex<NodeState>>) -> anyhow::Result<Self> {
        let msg: Message = serde_json::from_str(&line).context("Message deserialization error")?;

        match msg.body.clone() {
            MessageBody::Init {
                msg_id,
                node_id,
                node_ids,
            } => {
                let mut state_guard = state.lock().expect("State poisoned when executing init");

                state_guard.node_id = node_id.clone();
                state_guard.node_ids = node_ids;

                let node = Self {
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

    fn handle(&mut self, req: Message) -> anyhow::Result<Option<Message>> {
        let mut state = self
            .state
            .lock()
            .expect("State poisoned while building reply");

        let body: Option<MessageBody> = match req.body.clone() {
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
                state.gossip_messages.insert_pending(message);

                Some(MessageBody::BroadcastOk {
                    in_reply_to: msg_id,
                    msg_id: state.last_message_id + 1,
                })
            }
            MessageBody::Read { msg_id } => Some(MessageBody::ReadOk {
                msg_id: state.last_message_id + 1,
                messages: state.messages.clone(),
                in_reply_to: msg_id,
            }),
            MessageBody::Topology { msg_id, topology } => {
                state.neighbors = topology
                    .get(&state.node_id)
                    .with_context(|| format!("Node {} does not have topology", state.node_id))?
                    .to_vec();

                Some(MessageBody::TopologyOk {
                    msg_id: state.last_message_id + 1,
                    in_reply_to: msg_id,
                })
            }
            MessageBody::Gossip { msg_id, messages } => {
                let kwown_messages = state.messages.clone();

                // It adds to pending_gossips only the messages that the current node does not have
                state
                    .gossip_messages
                    .batch_insert_pending(messages.iter().filter(|m| !kwown_messages.contains(m)));
                state.messages.extend(messages.clone());

                Some(MessageBody::GossipOk {
                    msg_id: state.last_message_id + 1,
                    in_reply_to: msg_id,
                })
            }
            MessageBody::GossipOk {
                msg_id,
                in_reply_to: _,
            } => {
                // It acknowledge a message by removing it from gossip_messages.sent, so it is not necessary retry it.
                state.gossip_messages.remove_message_sent(msg_id);

                None
            }
            body => unimplemented!("Message {:?} not implemented yet", body),
        };

        match body {
            Some(b) => {
                state.last_message_id += 1;

                Ok(Some(Message {
                    src: state.node_id.clone(),
                    dest: req.src.clone(),
                    body: b,
                }))
            }
            None => Ok(None),
        }
    }

    fn write(&self, msg: Message) -> anyhow::Result<()> {
        let json = serde_json::to_string(&msg).context("Message serialization error")?;

        println!("{}", json);

        Ok(())
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
        msg_id: u32,
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
    Gossip {
        msg_id: u32,
        messages: HashSet<i32>,
    },
    GossipOk {
        msg_id: u32,
        in_reply_to: u32,
    },
}

#[derive(Debug)]
enum Event {
    // A node replies the request of a client.
    Reply(Message),
    // A node actively sends a message to a node or multiple nodes. An example
    // of this event would be sending a gossip message to node's neighbors.
    Push(Message),
    // A node should shutdown
    Shutdown,
}

fn main() -> anyhow::Result<()> {
    let state = Arc::new(Mutex::new(NodeState::default()));
    let mut first_line = String::new();

    // The first line must be a init, otherwise it returns an error.
    let stdin_state = state.clone();
    let mut node = match io::stdin().read_line(&mut first_line) {
        Ok(_) => Node::init(first_line, stdin_state)?,
        Err(_) => {
            panic!("Init message is required")
        }
    };
    let (tx, rx) = std::sync::mpsc::channel::<Event>();

    // Stdin thread
    let stdin_tx = tx.clone();
    thread::spawn(move || -> anyhow::Result<()> {
        let lines = io::stdin().lines();

        for line in lines {
            let content = line?;

            let msg: Message =
                serde_json::from_str(&content).context("Message deserialization error")?;

            stdin_tx
                .send(Event::Reply(msg))
                .context("Error when sending a Reply event")?;
        }

        stdin_tx
            .send(Event::Shutdown)
            .context("Error when sending a Shutdown event")?;

        Ok(())
    });

    // Gossip thread
    let gossip_tx = tx.clone();
    let gossip_state = state.clone();
    thread::spawn(move || -> anyhow::Result<()> {
        loop {
            thread::sleep(time::Duration::from_millis(200));

            let mut state_guard = gossip_state
                .lock()
                .expect("State poisoned while sending a gossip");

            if state_guard.gossip_messages.pending.is_empty() {
                continue;
            }

            let mut msg_id = state_guard.last_message_id;
            let neighbors = state_guard.neighbors.clone();

            for neighbor in neighbors {
                msg_id += 1;

                let message = Message {
                    src: state_guard.node_id.clone(),
                    dest: neighbor.to_string(),
                    body: MessageBody::Gossip {
                        msg_id: msg_id,
                        messages: state_guard.gossip_messages.pending.clone(),
                    },
                };
                gossip_tx
                    .send(Event::Push(message.clone()))
                    .context("Error when sending a Push event")?;

                state_guard.gossip_messages.insert_sent(msg_id, &message);
            }

            state_guard.last_message_id = msg_id;
            state_guard.gossip_messages.clear_pending();
        }
    });

    // Retry thread
    let retry_tx = tx.clone();
    let retry_state = state.clone();
    thread::spawn(move || -> anyhow::Result<()> {
        // It retries send failed messages every 400ms. Messages that failed are saved in
        // gossip_messages.sent. They are removed when they are acknowledge.
        loop {
            thread::sleep(time::Duration::from_millis(400));

            let state_guard = retry_state
                .lock()
                .expect("State poisoned while sending a retry");

            if state_guard.gossip_messages.sent.is_empty() {
                continue;
            }

            let sent_messages = state_guard.gossip_messages.sent.clone();

            for (_, msg) in sent_messages {
                retry_tx
                    .send(Event::Push(msg))
                    .context("Error when sending a Push event")?;
            }
        }
    });

    while let Ok(evt) = rx.recv() {
        match evt {
            Event::Reply(msg) => {
                if let Some(reply) = node.handle(msg.clone())? {
                    node.write(reply)?;
                }
            }
            Event::Push(msg) => {
                node.write(msg)?;
            }
            Event::Shutdown => break,
        }
    }

    Ok(())
}
