



## Adding itmes
* add item endpoint
    * adds item to this node memory as well as the _item_delta_ - a container to store items that have to be send to other nodes
* remove item endpoint
    * removed item from this node memory as well as adding to _item_delta_
        * process of removing an item is replacing existing item with item of type Thombstone

## Gossip threads
* send gossip
    * check whether there are some nodes that this node have not seen in a while, if so remove them from membership
    * make a list of N next nodes we should gossip with
        * send a message with current membership state together with list of _item_delta_
        * clear the list of _item_delta_ so that we do not keep sending it again

* receive gossip

## Concepts

* Adding Items:
When an item is added, it’s inserted into the store, items_delta, and items_delta_cache.

Delta State:
When sending deltas to peers, call is made to add_delta_state(), which adds the peer(s) to peers_pending for each item.

* Acknowledgment:
When a node receives an ACK, it removes the sender from peers_pending. If peers_pending is empty, the item is removed from items_delta and items_delta_state.

* Sending Deltas:
get_delta_for_node determines which items to send to each peer, using items_delta and items_delta_cache.