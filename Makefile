test-echo:
	cargo build --package echo --release
	./client/maelstrom test -w echo --bin ./target/release/echo --node-count 1 --time-limit 10

test-unique-id:
	cargo build --package unique-id --release
	./client/maelstrom test -w unique-ids --bin ./target/release/unique-id --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

test-broadcast-a:
	cargo build --package broadcast --release
	./client/maelstrom test -w broadcast --bin ./target/release/broadcast --node-count 1 --time-limit 20 --rate 10
	
test-broadcast-b:
	cargo build --package broadcast --release
	./client/maelstrom test -w broadcast --bin ./target/release/broadcast --node-count 5 --time-limit 20 --rate 10

test-broadcast-c:
	cargo build --package broadcast --release
	./client/maelstrom test -w broadcast --bin ./target/release/broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition

test-broadcast-d:
	cargo build --package broadcast --release
	./client/maelstrom test -w broadcast --bin ./target/release/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100 

test-broadcast-e:
	cargo build --package broadcast --release
	./client/maelstrom test -w broadcast --bin ./target/release/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100 

test-g-counter:
	cargo build --package g-counter --release
	./client/maelstrom test -w g-counter --bin ./target/release/g-counter --node-count 3 --time-limit 20 --rate 100 --nemesis partition 
