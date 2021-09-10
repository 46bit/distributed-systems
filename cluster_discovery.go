package main

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Node struct {
	Address string `yaml:"address"`
	ID      string `yaml:"id"`
}

type ClusterDiscovery interface {
	DiscoverNodes() ([]Node, error)
}

type ClusterDiscoveryFromFile struct {
	ConfigFile string
}

func (c *ClusterDiscoveryFromFile) DiscoverNodes() ([]Node, error) {
	type Config struct {
		Nodes []Node `yaml:"nodes"`
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
