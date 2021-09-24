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
	startTime *time.Time
}

var _ api.NodeServer = (*NodeServer)(nil)

func NewNodeServer(nodeId string, storage *Storage) *NodeServer {
	now := time.Now()
	return &NodeServer{
		nodeId:    nodeId,
		storage:   storage,
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
	keys, err := s.storage.Keys()
	if err != nil {
		err = fmt.Errorf("error listing keys: %w", err)
		fmt.Println(err)
		return err
	}

	return stream.Send(&api.InfoResponse{
		NodeId: s.nodeId,
		Uptime: durationpb.New(s.uptime()),
		Keys:   keys,
	})
}

func (s *NodeServer) Get(ctx context.Context, req *api.GetRequest) (*api.NodeGetResponse, error) {
	clockedEntry, err := s.storage.Get(req.Key)
	if err != nil {
		fmt.Println(fmt.Errorf("error getting value from node: %w", err))
		return nil, err
	}
	return &api.NodeGetResponse{ClockedEntry: clockedEntry}, nil
}

func (s *NodeServer) Set(ctx context.Context, req *api.NodeSetRequest) (*api.SetResponse, error) {
	err := s.storage.Set(req.ClockedEntry)
	if err != nil {
		fmt.Println(fmt.Errorf("error setting value on node: %w", err))
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
