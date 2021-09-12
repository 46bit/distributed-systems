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

type GossipSettings struct {
	GossipRegularity time.Duration
	NodeTimeoutAfter time.Duration
}

type Gossip struct {
	sync.Mutex
	GossipSettings

	Node              *Node
	OtherNodeStatuses map[NodeID]NodeGossip
}

type NodeGossip struct {
	LastSeenAt *time.Time
}

func NewGossip(node *Node, gossipSettings GossipSettings) *Gossip {
	gossip := &Gossip{
		GossipSettings:    gossipSettings,
		Node:              node,
		OtherNodeStatuses: map[NodeID]NodeGossip{},
	}
	for _, otherNode := range node.OtherNodes {
		gossip.OtherNodeStatuses[otherNode.ID] = NodeGossip{}
	}
	return gossip
}

func (g *Gossip) Summary() *GossipSummary {
	g.Lock()
	defer g.Unlock()

	seenWithinTimeout := 0
	for _, nodeStatus := range g.OtherNodeStatuses {
		if nodeStatus.LastSeenAt == nil {
			continue
		}
		age := time.Now().Sub(*nodeStatus.LastSeenAt)
		if age > g.NodeTimeoutAfter {
			continue
		}
		seenWithinTimeout += 1
	}

	// Adds 1 to number of nodes for the current node
	return &GossipSummary{
		ClusterNodeCount:       1 + len(g.Node.OtherNodes),
		OtherNodesSeenRecently: seenWithinTimeout,
	}
}

type GossipSummary struct {
	ClusterNodeCount       int
	OtherNodesSeenRecently int
}

func (g *GossipSummary) RecentlySawMostOfCluster() bool {
	nodesSeenIncludingItself := 1 + g.OtherNodesSeenRecently
	return nodesSeenIncludingItself > g.ClusterNodeCount/2
}

func (g *Gossip) Run() error {
	go gossipBroadcast(g)
	return gossipServer(g)
}

func gossipServer(g *Gossip) error {
	mux := http.NewServeMux()
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Println(fmt.Errorf("error reading from connection: %w", err))
			return
		}

		var gossipMessage gossipMessage
		if err = yaml.Unmarshal(bodyBytes, &gossipMessage); err != nil {
			log.Println(fmt.Errorf("error deserialising body: %w", err))
		}

		// FIXME: Authenticate nodes with Mutual TLS?
		g.Lock()
		_, ok := g.OtherNodeStatuses[gossipMessage.NodeID]
		if !ok {
			g.Unlock()
			log.Println(fmt.Errorf("error: received gossip message for unknown node id '%s'", gossipMessage.NodeID))
			return
		}
		nodeStatus := g.OtherNodeStatuses[gossipMessage.NodeID]
		now := time.Now()
		nodeStatus.LastSeenAt = &now
		g.OtherNodeStatuses[gossipMessage.NodeID] = nodeStatus
		g.Unlock()

		reply := newGossipReply(g)
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
	return http.ListenAndServe(g.Node.LocalAddress, mux)
}

func gossipBroadcast(g *Gossip) {
	// FIXME: Make gossip regularlity fixed so obviously safe to not lock?
	g.Lock()
	gossipRegularity := g.GossipRegularity
	g.Unlock()

	for range time.Tick(gossipRegularity) {
		gossipMessage := newGossipMessage(g)
		for _, otherNode := range g.Node.OtherNodes {
			err := sendGossipMessage(gossipMessage, otherNode.RemoteAddress)
			if err != nil {
				log.Println(fmt.Errorf("error sending gossip message to node '%s': %w", otherNode.ID, err))
			}
		}
	}
}

type gossipMessage struct {
	NodeID    NodeID    `yaml:"node_id"`
	Timestamp time.Time `yaml:"timestamp"`
}

func newGossipMessage(g *Gossip) *gossipMessage {
	g.Lock()
	defer g.Unlock()
	return &gossipMessage{
		NodeID:    g.Node.ID,
		Timestamp: time.Now(),
	}
}

type gossipReply struct {
	NodeID    NodeID    `yaml:"node_id"`
	Timestamp time.Time `yaml:"timestamp"`
}

func newGossipReply(g *Gossip) *gossipReply {
	g.Lock()
	defer g.Unlock()
	return &gossipReply{
		NodeID:    g.Node.ID,
		Timestamp: time.Now(),
	}
}

func sendGossipMessage(gossipMessage *gossipMessage, nodeAddress string) error {
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

	var gossipReply gossipReply
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
