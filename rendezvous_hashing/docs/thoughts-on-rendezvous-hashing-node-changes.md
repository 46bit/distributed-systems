When a new node joins, what happens?
All new read/writes will go to that new node
Separately, the pre-existing nodes will have to find content which belongs on
  that server.
Each data item will be persisted to the top N nodes.
  So N-1 nodes will 

If a server leaves expectedly, it should write all data to the a new node, to
maintain the N replication.
If a server leaves unexpectedly, the other nodes need to notice that its content
needs re-replicating. Find content that belonged on it.

That sounds quite expensive to me. O(number of items on the server that left)
when a server leaves. O(number of items in the cluster) when a server joins.




Do it passively:
- Every time an object is requested, the coordinator asks all N top nodes
  for it. It'll be found so long as you haven't recently added N nodes
  above the ones with an actual copy of the data. If still not found you
  could maaaybe do a broadcast asking for it but I dunno. Or just go
  further down.
- Existing servers regularly check that all their data is persisted
  enough, and write to other servers if not:
  - Iterate over all existing data over time
  - For each item, look at the list of known replicas. Remove any that
    are no longer present. Recompute the ideal replicas. If current
    replicas don't match known replicas, write to the ideal replicas
    and update the list.
  - If this node is no longer needed as a replica (not in the top N
    hashes) then can delete the data once replicated.
  - This means new servers will get N writes of everything, but it's a
    fair bit simpler than anything else?

It's frustrating to not have an immediate global/coordinator list of
which content is on which server. But that'd be a huge list and just
not distributed.

Instead each server will rereplicate by itself.

Data stored on each server:
- Key/value pairs, each with a list of the other known replica nodes
- List of other nodes, their hashes
- Liveness data for other nodes to use in eviction?

Resiliency?
- If a node disappears, every other node will stop receiving messages
  from it. They will know it is gone. Their replication maintainance
  process above will rereplicate the data to a new replica.
- If a network partition happens, some nodes will stop receiving
  messages from some other nodes.
- If a node is slow, that can be observed and traffic redirected to a
  replica. But data won't be *moved* unless the node is "gone."

If a node is "gone" but then returns, some servers may disagree on its
liveness for a short time. This is also true for longer periods of
time during network partitions.

During sustained network partitions, nodes continue to operate only so
long as they can see most of the network. They will rereplicate data
in the normal way.

If all copies of some data were out of contact with the network for
awhile, what happens when they rejoin? Nothing much. The data may need
moving to new/returned other servers, but only in the normal way.

Problem is, what if data is written while a node was out of contact?
It'll have gone to other servers. And when it returns it has an old
copy of data. Wellll, this is where stuff like vector clocks (or 
truetime…) come in. A way to resolve potential data duplicates.

It's not enough for a returning node to blindly write or accept one
copy of data. Forcing the returning data is broken – what if a new
version was written while the node was online. Accepting the other's
data is also broken – what if the server went offline before it got to
replicate new data? (Altho that's fixable if the coordinator writes
to all…)

OK, so ignore causality for now. Just get storage working and pretend
there's some external write-syncing.

What happens if a node receives a write? Does it need to check for a
better node or not?
