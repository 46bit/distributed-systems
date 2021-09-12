Thought exercise:

Some data is written to the cluster while fully online.
All the replicas of that data go down, but more than half the cluster
  is still online.
Right now the system would believe it should still be able to access
  that data. It would request it from the best online nodes, or worse
  write it to the best online nodes.

The system has to notice when data belongs on offline nodes. Question
  is, how does it know when a write is safe?
Maybe safety in this cluster is not about >50% of generic nodes being
  online. Maybe safety is about >50% of the best replicas for a piece
  of data being online!

I think that is correct. You can't set new data unless >50% of the
  best destination nodes are online. And you also can't get data
  without the risk it is stale.

The 50% here is fungible. If writes only succeed after data is 
  committed to all replicas, then it could be 1%. But the >50% ensures
  that we won't get split-brain partitions accepting new data.
Re-replicating data as nodes enter/leave may change some of this, but
  I am now convinced that I need to track >50% online nodes per key.

I am wondering whether I even need to ping other nodes to check they
  are reachable. If I require >50% of the best replicas for each piece
  of data to be online, then it's OK to check that while setting new
  data.
What about reads though? I really don't want to have to ask every
  replicas during every read! I do want to stop reads if I'm in a
  minority partition, to stop serving stale data, and pinging other
  nodes would detect that. Within a second or two.

Writes talk to all the replicas of that data. That's intentional and
  unavoidable. Reads talk to 1 replica, unless it's not found, which
  makes all the other replicas be checked.
I'm not entirely sure that data read from a single replica is certain
  to be up to date. If writes go through when only 50% of the replicas
  are online, then some replicas will miss new data.
Maybe I'm fighting the CAP tradeoff. That should be clearer when I
  look at how vector clocks will affect this.

I have so much to learn about distributed systems theory. If I truly
  understood CAP then I'd not be blustering about in this doc.
But my best guess:
- I have to successfully write data to >50% of replicas
- I have to successfully read data from >50% of replicas
- Maybe can alter those proportions just so long as it sums >=100%
- Vector clocks/etc will be used to figure out which returning replica
  has the latest written data
