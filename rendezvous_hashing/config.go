package rendezvous_hashing

import (
	"fmt"
	"io/ioutil"

	"github.com/spaolacci/murmur3"
	"gopkg.in/yaml.v2"
)

type NodeConfig struct {
	Id                  string `yaml:"id"`
	LocalAddress        string `yaml:"local_address"`
	LocalMetricsAddress string `yaml:"local_metrics_address"`
	BadgerDbFolder      string `yaml:"badger_db_folder"`
}

func LoadNodeConfig(path string) (*NodeConfig, error) {
	var nestedNodeConfig struct {
		Node NodeConfig `yaml:"node"`
	}
	yamlBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading node config file: %w", err)
	}
	if err = yaml.Unmarshal(yamlBytes, &nestedNodeConfig); err != nil {
		return nil, fmt.Errorf("error deserialising node config file: %w", err)
	}
	return &nestedNodeConfig.Node, nil
}

type ClusterConfig struct {
	Seed         uint32                      `yaml:"seed"`
	ReplicaCount int                         `yaml:"replica_count"`
	Nodes        map[string]*NodeDescription `yaml:"nodes"`
}

type NodeDescription struct {
	ID            string
	Hash          uint32
	RemoteAddress string `yaml:"remote_address"`
}

func LoadClusterConfig(path string) (*ClusterConfig, error) {
	var nestedClusterConfig struct {
		Cluster ClusterConfig `yaml:"cluster"`
	}
	yamlBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading cluster config file: %w", err)
	}
	if err = yaml.Unmarshal(yamlBytes, &nestedClusterConfig); err != nil {
		return nil, fmt.Errorf("error deserialising cluster config file: %w", err)
	}

	for id, node := range nestedClusterConfig.Cluster.Nodes {
		node.ID = id
		node.Hash = murmur3.Sum32WithSeed([]byte(id), nestedClusterConfig.Cluster.Seed)
	}
	return &nestedClusterConfig.Cluster, nil
}
