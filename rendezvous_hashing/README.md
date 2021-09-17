# Key-value store based upon Rendezvous Hashing

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

Load test with `ghz` (ingress at a single node):

```sh
ghz --insecure --async --proto ./api/api.proto --call api.Cluster/Set -n 20000 --rps=400 -d '{"entry": {"key": "{{.UUID}}", "value": "value of {{.UUID}}"}}' localhost:8001
```
