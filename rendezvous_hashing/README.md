# Leaderless Key-Value Store

This repo contains an example leaderless key-value store based on:

- [Rendezvous Hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing) to efficiently shard keys
- [Quorum Consistency](https://code.likeagirl.io/distributed-computing-quorum-consistency-in-replication-96e64f7b5c6) to tradeoff good consistency vs tolerating node failures
- [Quorum Clocks](http://rystsov.info/2018/10/01/tso.html) to logically order the data

Nodes are homogenous. Every node acts as both storage and coordinator. This means that any node can accept external queries.

APIs are implemented with [gRPC](https://grpc.io) and [Protobuf](https://developers.google.com/protocol-buffers). Nodes store their data using [BadgerDB](https://github.com/dgraph-io/badger).

## Status

A 5-node cluster can accept 1krps of 10KB writes with minimal errors. This is with 2 EPYC CPU cores per node. Performance appeared to be CPU-bound so higher performance should be easily obtainable.

This is not intended as a production commercial project. It is merely for learning. There are numerous `FIXME` comments throughout the codebase. Many suggest performance improvements but others deal with rare errorcases.

This does not implement data re-replication. Nodes that have been offline and missed some writes do not get that missing data synced to them. Similarly new nodes do not get existing data. This could be added.

## Notes

Regenerate protobuf Go code:

```sh
protoc -I=. --go_out=. --go-grpc_out=. api/api.proto
```

Start a 3-node local test server:

```sh
go run ./cmd/main/main.go ./examples/3-local-nodes/node-1.yml ./examples/3-local-nodes/cluster.yml
go run ./cmd/main/main.go ./examples/3-local-nodes/node-2.yml ./examples/3-local-nodes/cluster.yml
go run ./cmd/main/main.go ./examples/3-local-nodes/node-3.yml ./examples/3-local-nodes/cluster.yml
```

Interact manually with the Cluster API:

```sh
grpcurl -plaintext -d '{"key": "b"}' -proto api/api.proto localhost:8001 api.Cluster/Get
grpcurl -plaintext -d '{"entry": {"key": "b", "value": "b-value"}}' -proto api/api.proto localhost:8001 api.Cluster/Set
```

Interact manually with the Node API:

```sh
grpcurl -plaintext -d '{}' -proto api/api.proto localhost:8001 api.Node/Info
grpcurl -plaintext -d '{}' -proto api/api.proto localhost:8001 api.Node/Health
grpcurl -plaintext -d '{"key": "a"}' -proto api/api.proto localhost:8001 api.Node/Get
grpcurl -plaintext -d '{"entry": {"key": "a", "value": "a-value"}}' -proto api/api.proto localhost:8001 api.Node/Set
```

Load test with `ghz`:

```sh
ghz --rps=250 --duration 15s --skipFirst 1000 --insecure --connections 5 --proto ./api/api.proto --call api.Cluster/Set -d '{"entry": {"key": "{{.UUID}}", "value": "{{randomString 1024}}"}}' --lb-strategy=round_robin dns:///rz.46b.it:80
```
