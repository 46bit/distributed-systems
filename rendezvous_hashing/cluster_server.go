package rendezvous_hashing

import (
	"context"

	"github.com/46bit/distributed_systems/rendezvous_hashing/pb"
)

type ClusterServer struct {
	pb.UnimplementedClusterServer

	cluster *Cluster
}

var _ pb.ClusterServer = (*ClusterServer)(nil)

func NewClusterServer(cluster *Cluster) *ClusterServer {
	return &ClusterServer{cluster: cluster}
}

func (s *ClusterServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	entry, err := Read(req.Key, s.cluster)
	var pbEntry *pb.Entry
	if entry != nil {
		pbEntry = &pb.Entry{
			Key:   entry.Key,
			Value: entry.Value,
		}
	}
	return &pb.GetResponse{
		Entry: pbEntry,
	}, err
}

func (s *ClusterServer) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	entry := Entry{
		Key:   req.Entry.Key,
		Value: req.Entry.Value,
	}
	return &pb.SetResponse{}, Write(entry, s.cluster)
}
