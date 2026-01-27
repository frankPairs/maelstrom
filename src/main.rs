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

struct StarTopology<'a> {
    node_ids: &'a Vec<String>,
}

impl<'a> StarTopology<'a> {
    fn new(node_ids: &'a Vec<String>) -> Self {
        Self { node_ids }
    }

    fn get_topology(&self) -> HashMap<String, Vec<String>> {
        let mut topology: HashMap<String, Vec<String>> = HashMap::new();

        if self.node_ids.is_empty() {
            return topology;
        }

        let mut node_ids_iter = self.node_ids.iter();
        let leader = node_ids_iter.next().unwrap();
        let mut followers = Vec::new();

        for node_id in node_ids_iter {
            followers.push(node_id.clone());

            topology.insert(node_id.clone(), vec![leader.clone()]);
        }

        topology.insert(leader.clone(), followers);

        topology
    }
}

#[derive(Debug, Default)]
struct NodeState {
    messages: HashSet<i32>,
    /// It represents a vector of node ids. These nodes will be used for gossiping.
    neighbors: Vec<String>,
    last_message_id: u32,
    pending_to_send: HashSet<i32>,
}

#[derive(Debug, Clone)]
struct Node {
    node_id: String,
    node_ids: Vec<String>,
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

    fn handle(&mut self, req: Message) -> anyhow::Result<Option<Message>> {
        let topology = StarTopology::new(&self.node_ids).get_topology();

        let body: Option<MessageBody> = match req.body.clone() {
            MessageBody::Echo { msg_id, echo } => Some(MessageBody::EchoOk {
                in_reply_to: msg_id,
                echo,
            }),
            MessageBody::Generate { msg_id } => Some(MessageBody::GenerateOk {
                in_reply_to: msg_id,
                id: Uuid::new_v4(),
            }),
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
    let node_id = node.node_id.clone();
    thread::spawn(move || -> anyhow::Result<()> {
        loop {
            thread::sleep(time::Duration::from_millis(500));

            let state_guard = gossip_state
                .lock()
                .expect("State poisoned while sending a gossip");

            // If there are not neighbors, then we can close the thread
            if state_guard.neighbors.len() == 0 {
                break Ok(());
            }

            if state_guard.pending_to_send.is_empty() {
                continue;
            }

            let mut msg_id = state_guard.last_message_id;
            let neighbors = state_guard.neighbors.clone();

            for neighbor in neighbors {
                msg_id += 1;

                let message = Message {
                    src: node_id.clone(),
                    dest: neighbor.to_string(),
                    body: MessageBody::Gossip {
                        msg_id: msg_id,
                        messages: state_guard.pending_to_send.clone(),
                    },
                };
                gossip_tx
                    .send(Event::Push(message.clone()))
                    .context("Error when sending a Push event")?;
            }
        }
    });

    while let Ok(evt) = rx.recv() {
        match evt {
            Event::Reply(msg) => {
                if let Some(reply) = node.handle(msg)? {
                    node.write(reply)?;
                }
            }
            Event::Push(msg) => {
                node.write(msg.clone())?;

                if let Some(reply) = node.handle(msg)? {
                    node.write(reply)?;
                }
            }
            Event::Shutdown => break,
        }
    }

    Ok(())
}
