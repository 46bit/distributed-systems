package main

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type ClusterDiscovery interface {
	DiscoverNodes() ([]NodeDescription, error)
}

func indexNodes(nodes []NodeDescription) map[string]NodeDescription {
	indexed := make(map[string]NodeDescription, len(nodes))
	for _, node := range nodes {
		indexed[node.ID] = node
	}
	return indexed
}

type ClusterDiscoveryFromFile struct {
	ConfigFile string
}

func (c *ClusterDiscoveryFromFile) DiscoverNodes() ([]NodeDescription, error) {
	type Config struct {
		Nodes []NodeDescription `yaml:"nodes"`
	}

	yamlBytes, err := ioutil.ReadFile(c.ConfigFile)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	var config Config
	if err = yaml.Unmarshal(yamlBytes, &config); err != nil {
		return nil, fmt.Errorf("error deserialising config file: %w", err)
	}

	return config.Nodes, nil
}
