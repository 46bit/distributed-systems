package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

type NodeID = string

type GossipStatus struct {
	sync.Mutex

	NodeID           NodeID
	GossipRegularity time.Duration
	NodeTimeoutAfter time.Duration
	StartedAt        time.Time
	NodesStatus      map[NodeID]NodeGossipStatus
}

func NewGossipStatus(nodeID NodeID, nodes []Node, gossipRegularity time.Duration, nodeTimeoutAfter time.Duration) *GossipStatus {
	gossipStatus := &GossipStatus{
		NodeID:           nodeID,
		GossipRegularity: gossipRegularity,
		NodeTimeoutAfter: nodeTimeoutAfter,
		StartedAt:        time.Now(),
		NodesStatus:      map[NodeID]NodeGossipStatus{},
	}
	for _, node := range nodes {
		if node.ID == nodeID {
			continue
		}
		gossipStatus.NodesStatus[node.ID] = NodeGossipStatus{}
	}
	return gossipStatus
}

type GossipStatusSummary struct {
	NumberOfNodes             int
	SeenDirectlyWithinTimeout int
}

func (g *GossipStatusSummary) MostNodesSeenDirectlyWithinTimeout() bool {
	return g.SeenDirectlyWithinTimeout > g.NumberOfNodes/2
}

func (g *GossipStatus) Summary() *GossipStatusSummary {
	g.Lock()
	defer g.Unlock()

	// FIXME: Make clearer that the current node is automatically present?
	seenDirectlyWithinTimeout := 1
	for nodeID, nodeStatus := range g.NodesStatus {
		if nodeID == g.NodeID {
			continue
		}

		if nodeStatus.TimeOfLastDirectMessage == nil {
			continue
		}
		age := time.Now().Sub(*nodeStatus.TimeOfLastDirectMessage)
		if age > g.NodeTimeoutAfter {
			continue
		}
		seenDirectlyWithinTimeout += 1
	}

	// Adds 1 to number of nodes for the current node
	return &GossipStatusSummary{
		NumberOfNodes:             len(g.NodesStatus) + 1,
		SeenDirectlyWithinTimeout: seenDirectlyWithinTimeout,
	}
}

type NodeGossipStatus struct {
	TimeOfLastDirectMessage *time.Time
}

type GossipMessage struct {
	NodeID    NodeID    `yaml:"node_id"`
	Timestamp time.Time `yaml:"timestamp"`
}

func NewGossipMessage(gossipStatus *GossipStatus) *GossipMessage {
	gossipStatus.Lock()
	defer gossipStatus.Unlock()

	gossipMessage := &GossipMessage{
		NodeID:    gossipStatus.NodeID,
		Timestamp: time.Now(),
	}
	return gossipMessage
}

type GossipReply struct {
	NodeID    NodeID    `yaml:"node_id"`
	Timestamp time.Time `yaml:"timestamp"`
}

func NewGossipReply(gossipStatus *GossipStatus) *GossipReply {
	// FIXME: Make node ID fixed so obviously safe to not lock?
	gossipStatus.Lock()
	defer gossipStatus.Unlock()

	return &GossipReply{
		NodeID:    gossipStatus.NodeID,
		Timestamp: time.Now(),
	}
}

func Gossip(gossipStatus *GossipStatus, nodes []Node, localGossipAddress string) error {
	go gossipBroadcast(nodes, gossipStatus)
	return gossipServer(localGossipAddress, gossipStatus)
}

func gossipServer(localAddress string, gossipStatus *GossipStatus) error {
	mux := http.NewServeMux()
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Println(fmt.Errorf("error reading from connection: %w", err))
			return
		}

		var gossipMessage GossipMessage
		if err = yaml.Unmarshal(bodyBytes, &gossipMessage); err != nil {
			log.Println(fmt.Errorf("error deserialising body: %w", err))
		}

		// FIXME: Authenticate nodes with Mutual TLS?
		gossipStatus.Lock()
		_, ok := gossipStatus.NodesStatus[gossipMessage.NodeID]
		if !ok {
			gossipStatus.Unlock()
			log.Println(fmt.Errorf("error: received gossip message for unknown node id '%s'", gossipMessage.NodeID))
			return
		}
		nodeStatus := gossipStatus.NodesStatus[gossipMessage.NodeID]
		now := time.Now()
		nodeStatus.TimeOfLastDirectMessage = &now
		gossipStatus.NodesStatus[gossipMessage.NodeID] = nodeStatus
		gossipStatus.Unlock()

		reply := NewGossipReply(gossipStatus)
		replyBytes, err := yaml.Marshal(&reply)
		if err != nil {
			log.Println(fmt.Errorf("error serialising reply into YAML: %w", err))
			return
		}
		if _, err = w.Write(replyBytes); err != nil {
			log.Println(fmt.Errorf("error sending reply: %w", err))
			return
		}
	}))
	// FIXME: Configure aggressive timeouts etc
	return http.ListenAndServe(localAddress, mux)
}

func gossipBroadcast(nodes []Node, gossipStatus *GossipStatus) {
	// FIXME: Make gossip regularlity fixed so obviously safe to not lock?
	gossipStatus.Lock()
	gossipRegularity := gossipStatus.GossipRegularity
	nodeID := gossipStatus.NodeID
	gossipStatus.Unlock()

	for range time.Tick(gossipRegularity) {
		gossipMessage := NewGossipMessage(gossipStatus)
		for _, node := range nodes {
			if node.ID == nodeID {
				continue
			}
			err := sendGossipMessage(gossipMessage, node.Address)
			if err != nil {
				log.Println(fmt.Errorf("error sending gossip message to node '%s': %w", node.ID, err))
			}
		}
	}
}

func sendGossipMessage(gossipMessage *GossipMessage, nodeAddress string) error {
	url := fmt.Sprintf("http://%s/", nodeAddress)
	messageBytes, err := yaml.Marshal(gossipMessage)
	if err != nil {
		return fmt.Errorf("error serialising message into YAML: %w", err)
	}

	r, err := http.Post(url, "application/json", bytes.NewBuffer(messageBytes))
	if err != nil {
		return fmt.Errorf("error sending gossip message to node at '%s': %w", nodeAddress, err)
	}

	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("error reading from connection: %w", err)
	}

	var gossipReply GossipReply
	if err = yaml.Unmarshal(bodyBytes, &gossipReply); err != nil {
		return fmt.Errorf("error deserialising body: %w", err)
	}

	// FIXME: There's no real need for a reply. Just using Mutual TLS
	// with certs containing the Node ID would be enough to be sure it is
	// working properly.
	if gossipReply.NodeID == "" {
		return fmt.Errorf("gossip reply had no nodeid")
	}
	return nil
}
