package main

type Config struct {
	Cluster ClusterDescription `yaml:"cluster"`
}

type ClusterDescription struct {
	Seed         uint32            `yaml:"seed"`
	ReplicaCount int               `yaml:"replica_count"`
	Nodes        []NodeDescription `yaml:"nodes"`
}

type NodeDescription struct {
	ID            string `yaml:"id"`
	RemoteAddress string `yaml:"remote_address"`
}
