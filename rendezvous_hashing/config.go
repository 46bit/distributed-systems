package rendezvous_hashing

import (
	"github.com/spaolacci/murmur3"
)

type Config struct {
	Cluster ClusterDescription `yaml:"cluster"`
}

type ClusterDescription struct {
	Seed         uint32                      `yaml:"seed"`
	ReplicaCount int                         `yaml:"replica_count"`
	Nodes        map[string]*NodeDescription `yaml:"nodes"`
}

type NodeDescription struct {
	ID            string
	Hash          uint32
	RemoteAddress string `yaml:"remote_address"`
}

func (c *Config) Initialise() {
	for id, node := range c.Cluster.Nodes {
		node.ID = id
		node.Hash = murmur3.Sum32WithSeed([]byte(id), c.Cluster.Seed)
	}
}
