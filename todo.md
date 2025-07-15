RECENTLY DONE
* membership does not always registers new node 3+
    - issue was related to timinig of gossip, instead of taking update as is, now we're merging items
* fix web_port = 0 when initializing new node
    - when merging nodes, we ensure we update node with port = 0 to proper port
* Implemented delta state
* Implemented record updates via endpoint
* when adding items, ocasionally there is a deadlock
    - issue was with hardcoded web port to 3002 and routing logic issue

TODO
* ensure when retrieving an item for VNODE we also check active Nodes 
* when item is deleted it does not properly propagate
* cluster replicas don't seem to be evenly distributed