package rendezvous_hashing

import (
	"context"
	"time"

	"github.com/46bit/distributed_systems/rendezvous_hashing/pb"
	"google.golang.org/protobuf/types/known/durationpb"
)

type NodeServer struct {
	pb.UnimplementedNodeServer

	nodeId    string
	storage   *Storage
	cluster   *Cluster
	startTime *time.Time
}

var _ pb.NodeServer = (*NodeServer)(nil)

func NewNodeServer(nodeId string, storage *Storage, cluster *Cluster) *NodeServer {
	now := time.Now()
	return &NodeServer{
		nodeId:    nodeId,
		storage:   storage,
		cluster:   cluster,
		startTime: &now,
	}
}

func (s *NodeServer) Health(ctx context.Context, _ *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{
		NodeId: s.nodeId,
		Status: pb.Health_ONLINE,
		Uptime: durationpb.New(s.uptime()),
	}, nil
}

func (s *NodeServer) Info(_ *pb.InfoRequest, stream pb.Node_InfoServer) error {
	onlineNodes := []string{}
	s.cluster.Lock()
	for nodeId, _ := range s.cluster.OnlineNodes {
		onlineNodes = append(onlineNodes, nodeId)
	}
	s.cluster.Unlock()

	keys := []string{}
	s.storage.Lock()
	for key := range s.storage.Data {
		keys = append(keys, key)
	}
	s.storage.Unlock()

	return stream.Send(&pb.InfoResponse{
		NodeId:      s.nodeId,
		Uptime:      durationpb.New(s.uptime()),
		OnlineNodes: onlineNodes,
		Keys:        keys,
	})
}

func (s *NodeServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value := s.storage.Get(req.Key)
	var entry *pb.Entry
	if value != nil {
		entry = &pb.Entry{
			Key:   req.Key,
			Value: *value,
		}
	}
	return &pb.GetResponse{Entry: entry}, nil
}

func (s *NodeServer) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	s.storage.Set(&Entry{
		Key:   req.Entry.Key,
		Value: req.Entry.Value,
	})
	return &pb.SetResponse{}, nil
}

func (s *NodeServer) uptime() time.Duration {
	uptime := time.Duration(0)
	if s.startTime != nil {
		uptime = time.Now().Sub(*s.startTime)
	}
	return uptime
}