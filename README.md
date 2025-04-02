# gossipgrid

## Start first node

```bash
RUST_LOG=info cargo run 3001
```

## Start second node

```bash
RUST_LOG=info cargo run 3002 4110 127.0.0.1:4109
```

## get item status

```bash
curl -XGET http://127.0.0.1:3001/tasks
```

## submit item

```bash
curl -H "Content-Type: application/json" -XPOST http://127.0.0.1:3001/tasks -d '{"message": "foo"}'
```