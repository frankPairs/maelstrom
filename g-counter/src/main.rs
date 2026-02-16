mod node;
mod topologies;

use anyhow::Context;
use core::time;
use std::{
    io::{self},
    sync::{Arc, Mutex},
    thread,
};

use node::{Event, Message, MessageBody, Node, NodeState};

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
    let neighbors = node.get_neighbors()?;

    thread::spawn(move || -> anyhow::Result<()> {
        loop {
            thread::sleep(time::Duration::from_millis(250));

            let mut state_guard = gossip_state
                .lock()
                .expect("State poisoned while sending a gossip");

            // If there are not neighbors, then we can close the thread
            if neighbors.is_empty() {
                break Ok(());
            }

            let mut msg_id = state_guard.last_message_id;
            let neighbors = neighbors.clone();

            for neighbor in neighbors {
                msg_id += 1;

                let message = Message {
                    src: node_id.clone(),
                    dest: neighbor.to_string(),
                    body: MessageBody::Gossip {
                        msg_id: msg_id,
                        counter: state_guard.counter.clone(),
                    },
                };

                gossip_tx
                    .send(Event::Push(message.clone()))
                    .context("Error when sending a Push event")?;
            }

            state_guard.last_message_id = msg_id;
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
