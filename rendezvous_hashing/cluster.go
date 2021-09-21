package rendezvous_hashing

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/46bit/distributed_systems/rendezvous_hashing/api"
	"github.com/spaolacci/murmur3"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type Cluster struct {
	sync.Mutex
	ClusterConfig
}

func NewCluster(clusterConfig *ClusterConfig) *Cluster {
	return &Cluster{
		ClusterConfig: *clusterConfig,
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

	clockedEntries := make(chan *api.ClockedEntry, len(chosenReplicas))
	successfulReads := uint64(0)
	g := new(errgroup.Group)
	for _, chosenReplica := range chosenReplicas {
		node := chosenReplica.Node
		g.Go(func() error {
			clockedEntry, err := readEntryFromNode(key, node)
			if err == nil {
				atomic.AddUint64(&successfulReads, 1)
			}
			if err == nil && clockedEntry != nil {
				clockedEntries <- clockedEntry
			}
			return err
		})
	}
	err := g.Wait()
	if err != nil {
		log.Println(fmt.Errorf("error while reading from all replicas: %w", err))
	}
	close(clockedEntries)

	// To write durably, we must have a majority of replicas online.
	// FIXME: Analyse this properly
	if successfulReads <= uint64(len(chosenReplicas) / 2) {
		return nil, fmt.Errorf("could not read from a majority of replicas")
	}

	maxEpoch := uint64(0)
	maxClock := uint64(0)
	var newestValue *Entry
	for clockedEntry := range clockedEntries {
		newest := false
		if clockedEntry.Clock.Epoch > maxEpoch {
			maxEpoch = clockedEntry.Clock.Epoch
			maxClock = clockedEntry.Clock.Clock
			newest = true
		} else if clockedEntry.Clock.Epoch == maxEpoch && clockedEntry.Clock.Clock > maxClock {
			maxClock = clockedEntry.Clock.Clock
			newest = true
		}

		if newest {
			// FIXME: Suggestion in http://rystsov.info/2018/10/01/tso.html to use 
			// a hash of the current cluster node's ID as a consistent tiebreak for if 
			// multiple clocked values have the same clock
			newestValue = &Entry{
				Key: clockedEntry.Entry.Key,
				Value: clockedEntry.Entry.Value,
			}
		}
	}

	if newestValue == nil {
		return nil, fmt.Errorf("no value found")
	}
	return newestValue, nil
}

func readEntryFromNode(key string, node *NodeDescription) (*api.ClockedEntry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		node.RemoteAddress,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)),
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("did not connect: %v", err)
	}
	defer conn.Close()

	r, err := api.NewNodeClient(conn).Get(ctx, &api.GetRequest{Key: key})
	if r == nil {
		return nil, nil
	}
	return r.ClockedEntry, nil
}

func Write(entry Entry, cluster *Cluster) error {
	chosenReplicas := cluster.FindNodesForKey(entry.Key)
	if len(chosenReplicas) == 0 {
		return fmt.Errorf("no nodes found to accept key")
	}

	replicaClocks := []*api.ClockValue{}
	successfulClockGets := uint64(0)
	g := &errgroup.Group{}
	for _, chosenReplica := range chosenReplicas {
		node := chosenReplica.Node
		g.Go(func() error {
			nodeClockResponse, err := getClockFromNode(node)
			if err == nil {
				replicaClocks = append(replicaClocks, nodeClockResponse.Value)
				atomic.AddUint64(&successfulClockGets, 1)
			}
			return err
		})
	}
	if err := g.Wait(); err != nil {
		log.Println("error getting replica clocks: %w", err)
	}

	// To write durably, we must have a majority of replicas online.
	// FIXME: Analyse this properly
	if successfulClockGets <= uint64(len(chosenReplicas) / 2) {
		return fmt.Errorf("could not get clock from a majority of replicas")
	}

	maxEpoch := uint64(0)
	maxClock := uint64(0)
	for _, replicaClock := range replicaClocks {
		if replicaClock.Epoch > maxEpoch {
			maxEpoch = replicaClock.Epoch
			maxClock = replicaClock.Clock
		} else if replicaClock.Epoch == maxEpoch && replicaClock.Clock > maxClock {
			maxClock = replicaClock.Clock
		}
	}
	// FIXME: Add extra defensive check that epoch and clock not zero
	clockValue := api.ClockValue{
		Epoch: maxEpoch,
		Clock: maxClock + 1,
	}

	successfulClockSets := uint64(0)
	g = &errgroup.Group{}
	for _, chosenReplica := range chosenReplicas {
		node := chosenReplica.Node
		g.Go(func() error {
			err := setClockOnNode(clockValue, node)
			if err == nil {
				atomic.AddUint64(&successfulClockSets, 1)
			}
			return err
		})
	}
	if err := g.Wait(); err != nil {
		log.Println("error setting replica clocks: %w", err)
	}

	// To write durably, we must have a majority of replicas online.
	// FIXME: Analyse this properly
	if successfulClockSets <= uint64(len(chosenReplicas) / 2) {
		return fmt.Errorf("could not set clock on a majority of replicas")
	}

	clockedEntry := api.ClockedEntry{
		Entry: &api.Entry{
			Key: entry.Key,
			Value: entry.Value,
		},
		Clock: &clockValue,
	}
	successfulWrites := uint64(0)
	g = &errgroup.Group{}
	for _, chosenReplica := range chosenReplicas {
		node := chosenReplica.Node
		g.Go(func() error {
			err := writeEntryToNode(clockedEntry, node)
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

func writeEntryToNode(clockedEntry api.ClockedEntry, node *NodeDescription) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		node.RemoteAddress,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)),
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("did not connect: %v", err)
	}
	defer conn.Close()

	_, err = api.NewNodeClient(conn).Set(ctx, &api.NodeSetRequest{
		ClockedEntry: &clockedEntry,
	})
	return err
}

func getClockFromNode(node *NodeDescription) (*api.ClockGetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		node.RemoteAddress,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)),
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("did not connect: %v", err)
	}
	defer conn.Close()

	return api.NewClockClient(conn).Get(ctx, &api.ClockGetRequest{})
}

func setClockOnNode(clockValue api.ClockValue, node *NodeDescription) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		node.RemoteAddress,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)),
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("did not connect: %v", err)
	}
	defer conn.Close()

	_, err = api.NewClockClient(conn).Set(ctx, &api.ClockSetRequest{
		Value: &clockValue,
	})
	return err
}
