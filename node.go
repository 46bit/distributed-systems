package main

type NodeID = string

type Node struct {
	ID           NodeID
	LocalAddress string

	OtherNodes map[NodeID]NodeDescription
}

type NodeDescription struct {
	ID            NodeID `yaml:"id"`
	RemoteAddress string `yaml:"remote_address"`
}
