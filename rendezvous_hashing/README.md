# Key-value store based upon Rendezvous Hashing

Regenerate protobuf Go code:

```sh
protoc -I=. --go_out=. --go-grpc_out=. pb/api.proto
```

Start a 3-node local test server:

```sh
go run ./cmd/main/main.go ./examples/3-local-nodes/node-1.yml ./examples/3-local-nodes/cluster.yml
go run ./cmd/main/main.go ./examples/3-local-nodes/node-2.yml ./examples/3-local-nodes/cluster.yml
go run ./cmd/main/main.go ./examples/3-local-nodes/node-3.yml ./examples/3-local-nodes/cluster.yml
```

Interact manually with the Cluster API:

```sh
grpcurl -plaintext -d '{"key": "b"}' -proto pb/api.proto localhost:8001 pb.Cluster/Get
grpcurl -plaintext -d '{"entry": {"key": "b", "value": "b-value"}}' -proto pb/api.proto localhost:8001 pb.Cluster/Set
```

Interact manually with the Node API:

```sh
grpcurl -plaintext -d '{}' -proto pb/api.proto localhost:8001 pb.Node/Info
grpcurl -plaintext -d '{}' -proto pb/api.proto localhost:8001 pb.Node/Health
grpcurl -plaintext -d '{"key": "a"}' -proto pb/api.proto localhost:8001 pb.Node/Get
grpcurl -plaintext -d '{"entry": {"key": "a", "value": "a-value"}}' -proto pb/api.proto localhost:8001 pb.Node/Set
```
