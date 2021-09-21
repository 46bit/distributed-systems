module github.com/46bit/distributed_systems/rendezvous_hashing

go 1.15

replace github.com/dgraph-io/ristretto v0.1.0 => github.com/46bit/ristretto v0.1.0-with-arm-fix

require (
	github.com/dgraph-io/badger/v3 v3.2103.1
	github.com/golang/protobuf v1.5.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/prometheus/client_golang v1.11.0
	github.com/spaolacci/murmur3 v1.1.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/grpc v1.40.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/yaml.v2 v2.4.0
)
