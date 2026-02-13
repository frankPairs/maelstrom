mod node;

use anyhow::Context;
use node::{Message, Node};
use std::io::{self};

use crate::node::NodeState;

fn main() -> anyhow::Result<()> {
    let mut state = NodeState::default();
    let mut first_line = String::new();

    // The first line must be a init, otherwise it returns an error.
    let mut node = match io::stdin().read_line(&mut first_line) {
        Ok(_) => Node::init(first_line, &mut state)?,
        Err(_) => {
            panic!("Init message is required")
        }
    };

    let lines = io::stdin().lines();

    for line in lines {
        let content = line?;

        let req: Message =
            serde_json::from_str(&content).context("Message deserialization error")?;

        if let Some(res) = node.handle(req)? {
            node.write(res)?;
        }
    }

    Ok(())
}
