test-single-node:
	cargo build --release
	../client/maelstrom test -w broadcast --bin ./target/release/broadcast --node-count 1 --time-limit 20 --rate 10

test-multi-node:
	cargo build --release
	../client/maelstrom test -w broadcast --bin ./target/release/broadcast --node-count 1 --time-limit 20 --rate 10
