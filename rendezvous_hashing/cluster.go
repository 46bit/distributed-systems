package rendezvous_hashing

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/46bit/distributed_systems/rendezvous_hashing/pb"
	"github.com/spaolacci/murmur3"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type Cluster struct {
	sync.Mutex
	ClusterConfig

	Connections map[string]*grpc.ClientConn
}

func NewCluster(clusterConfig *ClusterConfig) *Cluster {
	return &Cluster{
		ClusterConfig: *clusterConfig,
		Connections:   map[string]*grpc.ClientConn{},
	}
}

type FoundNode struct {
	CombinedHash uint64
	Node         *NodeDescription
}

func (c *Cluster) FindNodesForKey(key string) []FoundNode {
	c.Lock()
	defer c.Unlock()

	keyBytes := []byte(key)

	// FIXME: Optimise? Avoid allocations, etc
	numberOfNodes := len(c.Nodes)
	combinedHashToNode := make(map[uint64]*NodeDescription, numberOfNodes)
	combinedHashes := []uint64{}
	for _, node := range c.Nodes {
		combinedHash := murmur3.Sum64WithSeed(keyBytes, node.Hash)
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

func Read(key string, cluster *Cluster) (*Entry, error) {
	chosenReplicas := cluster.FindNodesForKey(key)
	if len(chosenReplicas) == 0 {
		return nil, fmt.Errorf("no nodes found for key")
	}

	// Try to read from all replicas; succeed if more than half respond the same
	// FIXME: Implement conflict resolution so they don't have to respond the same
	entries := make(chan *Entry, len(chosenReplicas))
	g := new(errgroup.Group)
	for _, chosenReplica := range chosenReplicas {
		node := chosenReplica.Node
		g.Go(func() error {
			conn, err := cluster.Conn(node.RemoteAddress)
			if err != nil {
				return err
			}
			entry, err := readEntryFromNode(key, conn)
			if entry != nil {
				entries <- entry
			}
			return err
		})
	}
	err := g.Wait()
	if err != nil {
		log.Println(fmt.Errorf("error while reading from all replicas: %w", err))
	}
	close(entries)

	// FIXME: Go may be a painful language sometimes but there is surely
	// something I can do to improve this code
	valuesSeen := map[string]int{}
	for entry := range entries {
		if _, ok := valuesSeen[entry.Value]; !ok {
			valuesSeen[entry.Value] = 1
		} else {
			valuesSeen[entry.Value] += 1
		}
	}
	if len(valuesSeen) == 0 {
		return nil, err
	}
	maxTimesSeen := 0
	maxSeenValue := ""
	for valueSeen, timesSeen := range valuesSeen {
		if timesSeen > maxTimesSeen {
			maxTimesSeen = timesSeen
			maxSeenValue = valueSeen
		}
	}
	if maxTimesSeen > len(chosenReplicas)/2 {
		return &Entry{
			Key:   key,
			Value: maxSeenValue,
		}, nil
	}
	return nil, fmt.Errorf("failed to read same value from enough replicas (only %d of %d)", maxTimesSeen, len(chosenReplicas))
}

func readEntryFromNode(key string, conn *grpc.ClientConn) (*Entry, error) {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	r, err := pb.NewNodeClient(conn).Get(ctx, &pb.GetRequest{Key: key})
	var entry *Entry
	if r != nil && r.Entry != nil {
		entry = &Entry{
			Key:   r.Entry.Key,
			Value: r.Entry.Value,
		}
	}
	return entry, err
}

func Write(entry Entry, cluster *Cluster) error {
	chosenReplicas := cluster.FindNodesForKey(entry.Key)
	if len(chosenReplicas) == 0 {
		return fmt.Errorf("no nodes found to accept key")
	}

	successfulWrites := uint64(0)
	g := new(errgroup.Group)
	for _, chosenReplica := range chosenReplicas {
		node := chosenReplica.Node
		g.Go(func() error {
			conn, err := cluster.Conn(node.RemoteAddress)
			if err != nil {
				return err
			}
			err = writeEntryToNode(entry, conn)
			if err == nil {
				atomic.AddUint64(&successfulWrites, 1)
			}
			return err
		})
	}
	err := g.Wait()
	if successfulWrites > uint64(len(chosenReplicas)/2) {
		if err != nil {
			log.Println(err)
		}
		return nil
	}
	return err
}

func writeEntryToNode(entry Entry, conn *grpc.ClientConn) error {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := pb.NewNodeClient(conn).Set(ctx, &pb.SetRequest{
		Entry: &pb.Entry{
			Key:   entry.Key,
			Value: entry.Value,
		},
	})
	return err
}

func (c *Cluster) Conn(remoteAddress string) (*grpc.ClientConn, error) {
	c.Lock()
	defer c.Unlock()

	conn, ok := c.Connections[remoteAddress]
	if ok {
		return conn, nil
	}

	conn, err := connect(remoteAddress)
	if err != nil {
		return nil, fmt.Errorf("no existing connection to '%s' and could not open one: %w", remoteAddress, err)
	}
	return conn, nil
}

func connect(remoteAddress string) (*grpc.ClientConn, error) {
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	return grpc.DialContext(ctx, remoteAddress, grpc.WithInsecure(), grpc.WithBlock())
}
