package rendezvous_hashing

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/46bit/distributed_systems/rendezvous_hashing/api"
	"google.golang.org/grpc"
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
			if ok && lastSeen != nil {
				age := time.Now().Sub(*lastSeen)
				if age < l.NodeTimeoutAfter {
					l.Cluster.OnlineNodes[node.ID] = true
				} else {
					delete(l.Cluster.OnlineNodes, node.ID)
				}
			}
		}
		l.Cluster.Unlock()
		l.Unlock()
	}
}

func livenessBroadcast(l *Liveness) {
	// FIXME: Make gossip regularity fixed so obviously safe to not lock?
	l.Lock()
	gossipRegularity := l.GossipRegularity
	l.Unlock()

	for range time.Tick(gossipRegularity) {
		for _, otherNode := range l.Cluster.Nodes {
			r, err := getNodeHealth(otherNode.RemoteAddress)
			if err != nil {
				log.Println(fmt.Errorf("error sending liveness message to node '%s': %w", otherNode.ID, err))
				continue
			}
			if r.NodeId != otherNode.ID {
				log.Println(fmt.Errorf("health did not match node id, '%s' but expected '%s'", r.NodeId, otherNode.ID))
				continue
			}
			now := time.Now()
			l.Lock()
			l.NodesLastSeen[otherNode.ID] = &now
			l.Unlock()
		}
	}
}

func getNodeHealth(remoteAddress string) (*api.HealthResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, remoteAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("did not connect: %v", err)
	}
	defer conn.Close()

	return api.NewNodeClient(conn).Health(ctx, &api.HealthRequest{})
}
