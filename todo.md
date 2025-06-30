RECENT
* Implemented delta state
* Implemented record updates via endpoint

TODO
* when item is deleted it does not properly propagate
* ensure when retrieving an item for VNODE we also check active Nodes 
* cluster replicas don't seem to be evenly distributed
* when adding items, ocasionally there is a deadlock
* membership does not always registers new node 3+
* fix web_port = 0 when initializing new node