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

type LivenessSettings struct {
	GossipRegularity time.Duration
	NodeTimeoutAfter time.Duration
}

type Liveness struct {
	sync.Mutex
	LivenessSettings

	// FIXME: Stop needing to know current nodeid
	NodeID        string
	Cluster       *Cluster
	NodesLastSeen map[string]*time.Time
}

func NewLiveness(nodeID string, cluster *Cluster, settings LivenessSettings) *Liveness {
	liveness := &Liveness{
		NodeID:           nodeID,
		Cluster:          cluster,
		NodesLastSeen:    map[string]*time.Time{},
		LivenessSettings: settings,
	}
	// FIXME: Support `Cluster` adding/removing nodes
	for _, node := range cluster.Nodes {
		liveness.NodesLastSeen[node.ID] = nil
	}
	return liveness
}

func (l *Liveness) Run() {
	go livenessBroadcast(l)
	for range time.Tick(1 * time.Second) {
		l.Lock()
		l.Cluster.Lock()
		for _, node := range l.Cluster.Nodes {
			// FIXME: Handle nodes being added/removed from Cluster
			lastSeen, ok := l.NodesLastSeen[node.ID]
			if !ok {
				log.Fatal("unimplemented")
			}
			if lastSeen == nil {
				fmt.Printf("LIVENESS: %s never seen\n", node.ID)
			} else {
				age := time.Now().Sub(*lastSeen)
				if age < l.NodeTimeoutAfter {
					l.Cluster.OnlineNodes[node.ID] = true
					fmt.Printf("LIVENESS: %s seen recently\n", node.ID)
				} else {
					delete(l.Cluster.OnlineNodes, node.ID)
					fmt.Printf("LIVENESS: %s not seen recently\n", node.ID)
				}
			}
		}
		l.Cluster.Unlock()
		l.Unlock()
	}
}

func LivenessHandler(l *Liveness) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// FIXME: Configure aggressive timeouts etc
		bodyBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(500)
			log.Println(fmt.Errorf("error reading from connection: %w", err))
			return
		}

		var livenessMessage livenessMessage
		if err = yaml.Unmarshal(bodyBytes, &livenessMessage); err != nil {
			w.WriteHeader(500)
			log.Println(fmt.Errorf("error deserialising body: %w", err))
		}

		// FIXME: Authenticate nodes with Mutual TLS?
		l.Lock()
		_, ok := l.NodesLastSeen[livenessMessage.NodeID]
		if !ok {
			l.Unlock()
			w.WriteHeader(500)
			log.Println(fmt.Errorf("error: received liveness message for unknown node id '%s'", livenessMessage.NodeID))
			return
		}
		now := time.Now()
		l.NodesLastSeen[livenessMessage.NodeID] = &now
		l.Unlock()

		reply := newLivenessReply(l)
		replyBytes, err := yaml.Marshal(&reply)
		if err != nil {
			w.WriteHeader(500)
			log.Println(fmt.Errorf("error serialising reply into YAML: %w", err))
			return
		}
		if _, err = w.Write(replyBytes); err != nil {
			w.WriteHeader(500)
			log.Println(fmt.Errorf("error sending reply: %w", err))
			return
		}
	}
}

func livenessBroadcast(l *Liveness) {
	// FIXME: Make gossip regularity fixed so obviously safe to not lock?
	l.Lock()
	gossipRegularity := l.GossipRegularity
	l.Unlock()

	for range time.Tick(gossipRegularity) {
		livenessMessage := newLivenessMessage(l)
		for _, otherNode := range l.Cluster.Nodes {
			err := sendLivenessMessage(livenessMessage, otherNode.RemoteAddress)
			if err != nil {
				log.Println(fmt.Errorf("error sending liveness message to node '%s': %w", otherNode.ID, err))
			}
		}
	}
}

type livenessMessage struct {
	NodeID    string    `yaml:"node_id"`
	Timestamp time.Time `yaml:"timestamp"`
}

func newLivenessMessage(l *Liveness) *livenessMessage {
	l.Lock()
	defer l.Unlock()
	return &livenessMessage{
		NodeID:    l.NodeID,
		Timestamp: time.Now(),
	}
}

type livenessReply struct {
	NodeID    string    `yaml:"node_id"`
	Timestamp time.Time `yaml:"timestamp"`
}

func newLivenessReply(l *Liveness) *livenessReply {
	l.Lock()
	defer l.Unlock()
	return &livenessReply{
		NodeID:    l.NodeID,
		Timestamp: time.Now(),
	}
}

func sendLivenessMessage(livenessMessage *livenessMessage, nodeAddress string) error {
	url := fmt.Sprintf("http://%s/node/live", nodeAddress)
	messageBytes, err := yaml.Marshal(livenessMessage)
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

	var livenessReply livenessReply
	if err = yaml.Unmarshal(bodyBytes, &livenessReply); err != nil {
		return fmt.Errorf("error deserialising body: %w", err)
	}

	// FIXME: There's no real need for a reply. Just using Mutual TLS
	// with certs containing the Node ID would be enough to be sure it is
	// working properly.
	if livenessReply.NodeID == "" {
		return fmt.Errorf("liveness reply had no nodeid")
	}
	return nil
}
