package main

import (
	"sort"
	"sync"

	"github.com/spaolacci/murmur3"
)

type Node struct {
	ID            string
	Hash          uint64
	RemoteAddress string
}

type Cluster struct {
	sync.Mutex

	Seed         uint32
	ReplicaCount int
	KnownNodes   map[string]*Node
	OnlineNodes  map[string]*Node
}

func NewCluster(clusterDesc *ClusterDescription) *Cluster {
	nodes := make(map[string]*Node, len(clusterDesc.Nodes))
	for _, nodeDesc := range clusterDesc.Nodes {
		nodes[nodeDesc.ID] = &Node{
			ID:            nodeDesc.ID,
			Hash:          murmur3.Sum64WithSeed([]byte(nodeDesc.ID), clusterDesc.Seed),
			RemoteAddress: nodeDesc.RemoteAddress,
		}
	}

	return &Cluster{
		Seed:         clusterDesc.Seed,
		ReplicaCount: clusterDesc.ReplicaCount,
		KnownNodes:   nodes,
		OnlineNodes:  map[string]*Node{},
	}
}

func (c *Cluster) MostOfClusterIsOnline() bool {
	c.Lock()
	defer c.Unlock()
	return len(c.OnlineNodes) > len(c.KnownNodes)/2
}

type FoundNode struct {
	CombinedHash uint64
	Node         *Node
}

func (c *Cluster) FindNodesForKey(key string) []FoundNode {
	c.Lock()
	defer c.Unlock()

	// FIXME: Find a way to avoid copy here
	keyHash := murmur3.Sum64WithSeed([]byte(key), c.Seed)

	// FIXME: Optimise? Avoid allocations, etc
	numberOfOnlineNodes := len(c.OnlineNodes)
	combinedHashToNode := make(map[uint64]string, numberOfOnlineNodes)
	combinedHashes := []uint64{}
	for _, node := range c.OnlineNodes {
		combinedHash := keyHash ^ node.Hash
		combinedHashToNode[combinedHash] = node.ID
		combinedHashes = append(combinedHashes, combinedHash)
	}
	// Sort combined hashes into descending order
	sort.Slice(combinedHashes, func(i, j int) bool { return combinedHashes[i] > combinedHashes[j] })

	bestNodes := make([]FoundNode, c.ReplicaCount)
	for i := 0; i < c.ReplicaCount; i++ {
		combinedHash := combinedHashes[i]
		bestNodes[i] = FoundNode{
			CombinedHash: combinedHash,
			Node:         c.OnlineNodes[combinedHashToNode[combinedHash]],
		}
	}
	return bestNodes
}
