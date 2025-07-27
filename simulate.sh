#!/bin/bash
set -x
INTERVAL=1
ID=NONE

curl -H "Content-Type: application/json" -XPOST http://127.0.0.1:3001/items -d '{"id": "123", "message": "foo1"}'
sleep $INTERVAL
curl -H "Content-Type: application/json" -XPOST http://127.0.0.1:3002/items -d '{"id": "124", "message": "foo2"}'
sleep $INTERVAL
curl -H "Content-Type: application/json" -XPOST http://127.0.0.1:3003/items -d '{"id": "125", "message": "foo3"}'
# sleep $INTERVAL
# curl -H "Content-Type: application/json" -XPUT http://127.0.0.1:3002/items/123 -d '{"message": "foo4"}'