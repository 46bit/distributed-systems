Right now I extremely favour read availability over write availability
A single node failure disables R/N writes:
- 2 replicas and 3 nodes: 2/3 writes disabled by one node failure
- 2 replicas and 15 nodes: 2/15 writes disabled by one node failure
In a larger cluster reads are ridiculously available:
- 2 replicas and 3 nodes: 0 reads disabled by one node failure
                          1/3 reads disabled by two node failures
- 2 replicas and 15 nodes: 0 reads disabled by one node failure
                           1/15 reads disabled by worst two node failures
                           0 reads disabled by average two node failures

This is a bit excessive for a general-purpose system, although it is
  pretty cool how cheap reads are when I can try to read from a single
  node!
Better choice is for the system to keep working with most nodes (>50%)
  online. Writes succeed if >50% of replicas succeed. Reads succeed if
  >50% of replicas succeed (and for now, respond with the same value.)

DECISION:
  Writes:
  - Try to write to all replicas for that key
  - Succeed if >50% (most) of those writes succeed
  - Rollback to successful nodes if too many failed
  Reads:
  - Try to read from all replicas of that key
  - Succeed if >50% (most) of those reads succeed
  - For now, fail unless >50% of replicas agreed on same value
