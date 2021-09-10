package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	// FIXME: Take config better
	configFilePath := os.Args[1]
	nodeID := os.Args[2]
	// FIXME: Configure node local address better
	localAddress := ":" + nodeID

	clusterDiscovery := ClusterDiscoveryFromFile{ConfigFile: configFilePath}
	nodeList, err := clusterDiscovery.DiscoverNodes()
	if err != nil {
		log.Fatal(fmt.Errorf("error discovering nodes: %w", err))
	}

	otherNodes := indexNodes(nodeList)
	delete(otherNodes, nodeID)
	node := &Node{
		ID:           nodeID,
		LocalAddress: localAddress,
		OtherNodes:   otherNodes,
	}

	gossipSettings := GossipSettings{
		GossipRegularity: 1 * time.Second,
		NodeTimeoutAfter: 2 * time.Second,
	}
	gossip := NewGossip(node, gossipSettings)
	go func() {
		if err := gossip.Run(); err != nil {
			log.Fatal(fmt.Errorf("error in gossiping: %w", err))
		}
	}()

	for range time.Tick(1 * time.Second) {
		summary := gossip.Summary()
		fmt.Printf("RecentlySawMostOfCluster=%v %#v\n", summary.RecentlySawMostOfCluster(), summary)
	}
}
