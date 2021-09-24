package rendezvous_hashing

import (
	"fmt"
	"context"

	"github.com/46bit/distributed_systems/rendezvous_hashing/api"
)

type ClusterServer struct {
	api.UnimplementedClusterServer

	cluster *Cluster
}

var _ api.ClusterServer = (*ClusterServer)(nil)

func NewClusterServer(cluster *Cluster) *ClusterServer {
	return &ClusterServer{cluster: cluster}
}

func (s *ClusterServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	entry, err := Read(req.Key, s.cluster)
	if err != nil {
		fmt.Println(fmt.Errorf("error getting value from cluster: %w", err))
	}
	var pbEntry *api.Entry
	if entry != nil {
		pbEntry = &api.Entry{
			Key:   entry.Key,
			Value: entry.Value,
		}
	}
	return &api.GetResponse{
		Entry: pbEntry,
	}, err
}

func (s *ClusterServer) Set(ctx context.Context, req *api.SetRequest) (*api.SetResponse, error) {
	entry := Entry{
		Key:   req.Entry.Key,
		Value: req.Entry.Value,
	}
	err := Write(entry, s.cluster)
	if err != nil {
		fmt.Println(fmt.Errorf("error setting value in cluster: %w", err))
	}
	return &api.SetResponse{}, err
}
