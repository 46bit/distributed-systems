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
	gossipRegularity := 1 * time.Second
	nodeTimeoutAfter := 2 * gossipRegularity

	clusterDiscovery := ClusterDiscoveryFromFile{ConfigFile: configFilePath}
	nodes, err := clusterDiscovery.DiscoverNodes()
	if err != nil {
		log.Fatal(fmt.Errorf("error discovering nodes: %w", err))
	}

	gossipStatus := NewGossipStatus(nodeID, nodes, gossipRegularity, nodeTimeoutAfter)
	go func() {
		// FIXME: Configure node local address better
		err := Gossip(gossipStatus, nodes, ":"+nodeID)
		if err != nil {
			log.Fatal(fmt.Errorf("error in gossiping: %w", err))
		}
	}()

	for range time.Tick(1 * time.Second) {
		gossipSummary := gossipStatus.Summary()
		fmt.Printf("CanSeeMostOfCluster=%v %#v\n", gossipSummary.CanSeeMostOfCluster(), gossipSummary)
	}
}
