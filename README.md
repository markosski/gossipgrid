# gossipgrid

Implementation of gossip protocol:
- nodes can join the cluster if they know one other existing node
- nodes allow to submit, retrieve and remove messages/items replicated in the cluster through http server


## Start example

- first argument is required representing web server port
- default port is 4109, you can change it by providing a second argument as port value
- this argument is ip:port of existing node in the gossip network (needed to join the network)

### Start first node

```bash
RUST_LOG=info cargo run 3001
```

### Start second node

```bash
RUST_LOG=info cargo run 3002 4110 127.0.0.1:4109
```

### get item status

```bash
curl -XGET http://127.0.0.1:3001/items
```

### submit item

```bash
curl -H "Content-Type: application/json" -XPOST http://127.0.0.1:3001/items -d '{"message": "foo"}'
```

### delete existing item

```bash
curl -XDELETE http://127.0.0.1:3001/items/<ITEM_ID>
```

## Tests

```bash
cargo test --lib
```

### Run specific test

```bash
cargo test --package gossipgrid --test int_test test_publish_and_retrieve_item -- --nocapture

# or

cargo test --package gossipgrid --test int_test -- --test-threads=1 --nocapture
```

## CLI

```
gossipgrid cluster 3 --web-port 3001 --node-port 4109

gossipgrid join 127.0.0.1:4109 --wep-port 3002 --node-port 4110
```