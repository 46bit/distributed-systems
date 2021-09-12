package main

import (
	"sort"
	"sync"

	"github.com/spaolacci/murmur3"
)

type Cluster struct {
	sync.Mutex
	ClusterDescription

	OnlineNodes map[string]bool
}

func NewCluster(clusterDesc *ClusterDescription) *Cluster {
	return &Cluster{
		ClusterDescription: *clusterDesc,
		OnlineNodes:        map[string]bool{},
	}
}

type FoundNode struct {
	CombinedHash uint64
	Node         *NodeDescription
}

func (c *Cluster) FindNodesForKey(key string) []FoundNode {
	c.Lock()
	defer c.Unlock()

	// FIXME: Find a way to avoid copy here
	keyHash := murmur3.Sum64WithSeed([]byte(key), c.Seed)

	// FIXME: Optimise? Avoid allocations, etc
	numberOfNodes := len(c.Nodes)
	combinedHashToNode := make(map[uint64]*NodeDescription, numberOfNodes)
	combinedHashes := []uint64{}
	for _, node := range c.Nodes {
		combinedHash := keyHash ^ node.Hash
		combinedHashToNode[combinedHash] = node
		combinedHashes = append(combinedHashes, combinedHash)
	}
	// Sort combined hashes into descending order
	sort.Slice(combinedHashes, func(i, j int) bool { return combinedHashes[i] > combinedHashes[j] })

	bestNodes := make([]FoundNode, c.ReplicaCount)
	for i := 0; i < c.ReplicaCount; i++ {
		combinedHash := combinedHashes[i]
		bestNodes[i] = FoundNode{
			CombinedHash: combinedHash,
			Node:         combinedHashToNode[combinedHash],
		}
	}
	return bestNodes
}
