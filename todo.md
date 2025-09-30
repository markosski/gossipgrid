RECENTLY DONE
* membership does not always registers new node 3+
    - issue was related to timinig of gossip, instead of taking update as is, now we're merging items
* fix web_port = 0 when initializing new node
    - when merging nodes, we ensure we update node with port = 0 to proper port
* Implemented delta state
* Implemented record updates via endpoint
* when adding items, ocasionally there is a deadlock
    - issue was with hardcoded web port to 3002 and routing logic issue
* when node is only aware of another node, do not remove it from cluster, try to connect to it indefinitely
* create cli
* make it so client provide a key instead of it being generated
* consider creating cluster config ahead of time instead of forming it on the fly
* partition map does not recalculate to include newly joined node
* rejection join to cluster that is full
* delta propagation seems doing some ping/pong action
    * it seems like the node that sends immediate gossip receives acks properly
        * verify if other nodes only propagate delta through intervals (and if this is intentional), if so see if they are not sending Ack
    * A: this was due to Acks not being always sent if node already received an update from other node
* ensure all web endpoints have proxy
* when item is deleted it does not properly propagate
    * item count should exclude deleted items
* implement proper count
    * each node should gossip partitions and count, partitions should handle deleted items
* add more tests for cluster item count that ensures deleted items are not countedß
* implement better interface with Result type
* migrate away from Vnode references to PartitionId
* ensure base64 encoding when returning item
    * add api param to convert bytes to string
* change store implementation to return list of items
    * considerations for sort key / composite key, what order guarantees should we provide

TODO
* take a second look at the delta sync for proper implementation, currently sync flag is false 
* ensure when retrieving an item for VNODE we also check active Nodes 
* node that is in sync mode should not be used for reads until changes state
* cluster replicas don't seem to be evenly distributed
* create a most robust mechanisms for syncing items when node re-joins
    - ensure efficiency, micro batch mode with ack tracking and rejection of upstream updates
