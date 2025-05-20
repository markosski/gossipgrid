#!/bin/bash
set -x
INTERVAL=1
ID=NONE

curl -H "Content-Type: application/json" -XPOST http://127.0.0.1:3001/items -d '{"message": "foo1"}'
sleep $INTERVAL
ID=`curl -H "Content-Type: application/json" -XPOST http://127.0.0.1:3002/items -d '{"message": "foo2"}' | jq -r '.success.id'`
sleep $INTERVAL
curl -H "Content-Type: application/json" -XPOST http://127.0.0.1:3003/items -d '{"message": "foo3"}'
sleep $INTERVAL
curl -H "Content-Type: application/json" -XPATCH "http://127.0.0.1:3002/items/${ID}" -d '{"message": "foo4"}'