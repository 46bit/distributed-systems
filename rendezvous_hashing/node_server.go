package rendezvous_hashing

import (
	"context"
	"fmt"
	"time"

	"github.com/46bit/distributed_systems/rendezvous_hashing/api"
	"google.golang.org/protobuf/types/known/durationpb"
)

type NodeServer struct {
	api.UnimplementedNodeServer

	nodeId    string
	storage   *Storage
	cluster   *Cluster
	startTime *time.Time
}

var _ api.NodeServer = (*NodeServer)(nil)

func NewNodeServer(nodeId string, storage *Storage, cluster *Cluster) *NodeServer {
	now := time.Now()
	return &NodeServer{
		nodeId:    nodeId,
		storage:   storage,
		cluster:   cluster,
		startTime: &now,
	}
}

func (s *NodeServer) Health(ctx context.Context, _ *api.HealthRequest) (*api.HealthResponse, error) {
	return &api.HealthResponse{
		NodeId: s.nodeId,
		Status: api.Health_ONLINE,
		Uptime: durationpb.New(s.uptime()),
	}, nil
}

func (s *NodeServer) Info(_ *api.InfoRequest, stream api.Node_InfoServer) error {
	onlineNodes := []string{}
	s.cluster.Lock()
	for nodeId, _ := range s.cluster.OnlineNodes {
		onlineNodes = append(onlineNodes, nodeId)
	}
	s.cluster.Unlock()

	keys, err := s.storage.Keys()
	if err != nil {
		return fmt.Errorf("error listing keys: %w", err)
	}

	return stream.Send(&api.InfoResponse{
		NodeId:      s.nodeId,
		Uptime:      durationpb.New(s.uptime()),
		OnlineNodes: onlineNodes,
		Keys:        keys,
	})
}

func (s *NodeServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	value, err := s.storage.Get(req.Key)
	if err != nil {
		return nil, err
	}

	var entry *api.Entry
	if value != nil {
		entry = &api.Entry{
			Key:   req.Key,
			Value: *value,
		}
	}
	return &api.GetResponse{Entry: entry}, nil
}

func (s *NodeServer) Set(ctx context.Context, req *api.SetRequest) (*api.SetResponse, error) {
	err := s.storage.Set(&Entry{
		Key:   req.Entry.Key,
		Value: req.Entry.Value,
	})
	if err != nil {
		return nil, err
	}
	return &api.SetResponse{}, nil
}

func (s *NodeServer) uptime() time.Duration {
	uptime := time.Duration(0)
	if s.startTime != nil {
		uptime = time.Now().Sub(*s.startTime)
	}
	return uptime
}
