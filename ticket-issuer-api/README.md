# ticket-issuer-api

This isn't a from-scratch distributed system, but it's something from a system design problem I've been thinking about:

> Say you need to run a giveaway of X million items. You expect it to only take minutes to run out, so long-term persistence isn't an issue. Each user must only get one free item. How would you design the system?

I was curious how well I could do with Redis. Came up with a nice solution that pins particular user IDs to particular Redis shards, then uses Redis transactions and atomic incrementing to pretty efficiently assign tickets. You can get the gist of it from `ticket_issuer_server.go`.

It's pretty rare you'd go with this exact approach. It doesn't handle increasing the giveaway, for instance.