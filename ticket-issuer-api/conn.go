package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	MAX_MSG_SIZE = 64 << 20

	PING_RATE    = 5 * time.Second
	PING_TIMEOUT = 5 * time.Second
)

func NewGrpcServer(extraServerOptions ...grpc.ServerOption) *grpc.Server {
	// keepaliveEnforcementPolicy := keepalive.EnforcementPolicy{
	// 	PermitWithoutStream: true,
	// 	MinTime:             PING_RATE / 10,
	// }
	keepaliveServerParams := keepalive.ServerParameters{
		MaxConnectionIdle: 15 * time.Second,
		Time:              PING_RATE,
		Timeout:           PING_TIMEOUT,
	}
	serverOptions := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(1024),
		grpc.MaxRecvMsgSize(MAX_MSG_SIZE),
		grpc.MaxSendMsgSize(MAX_MSG_SIZE),
		grpc.KeepaliveParams(keepaliveServerParams),
		// grpc.KeepaliveEnforcementPolicy(keepaliveEnforcementPolicy),
	}
	serverOptions = append(serverOptions, extraServerOptions...)
	return grpc.NewServer(serverOptions...)
}

type ConnManager struct {
	sync.Mutex
	PoolSize          int
	RemoveUnusedAfter time.Duration
	Pools             map[string]ConnPool
	LastUsed          map[string]time.Time
}

func NewConnManager(poolSize int, removeUnusedAfter time.Duration) *ConnManager {
	if poolSize < 1 {
		poolSize = 1
	}
	return &ConnManager{
		PoolSize:          poolSize,
		RemoveUnusedAfter: removeUnusedAfter,
		Pools:             map[string]ConnPool{},
		LastUsed:          map[string]time.Time{},
	}
}

func (m *ConnManager) Add(ctx context.Context, address string, allowRemovalIfUnused bool) error {
	m.Lock()
	defer m.Unlock()
	_, ok := m.Pools[address]
	if ok {
		return nil
	}

	log.Println("adding managed connections to", address)
	pool, err := NewRoundRobinConnPool(ctx, address, m.PoolSize)
	if err != nil {
		return err
	}

	m.Pools[address] = pool
	if allowRemovalIfUnused {
		m.LastUsed[address] = time.Now()
	}
	log.Println("added managed connections to", address)
	return nil
}

func (m *ConnManager) Get(address string) (conn grpc.ClientConnInterface, ok bool) {
	m.Lock()
	defer m.Unlock()
	pool, ok := m.Pools[address]
	if ok {
		m.LastUsed[address] = time.Now()
	}
	return pool, ok
}

func (m *ConnManager) Run(ctx context.Context) {
	if m.RemoveUnusedAfter == 0 {
		return
	}
	ticker := time.NewTicker(m.RemoveUnusedAfter)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.garbageCollect()
		}
	}
}

func (m *ConnManager) garbageCollect() {
	m.Lock()
	defer m.Unlock()
	for address, lastUsed := range m.LastUsed {
		if time.Since(lastUsed) < m.RemoveUnusedAfter {
			continue
		}
		pool := m.Pools[address]
		errs := pool.Close()
		log.Println(fmt.Errorf("ignored errors closing connection pool: %v", errs))
		delete(m.Pools, address)
		delete(m.LastUsed, address)
	}
}

type ConnPool interface {
	grpc.ClientConnInterface

	Conn() *grpc.ClientConn
	PoolSize() int
	Close() []error
}

type RoundRobinConnPool struct {
	Index       int32
	Size        int
	Connections []*grpc.ClientConn
}

var _ (ConnPool) = &RoundRobinConnPool{}

func NewRoundRobinConnPool(ctx context.Context, address string, poolSize int) (*RoundRobinConnPool, error) {
	log.Println("creating round-robin connection pool to", address, "with size", poolSize)
	conns := make([]*grpc.ClientConn, poolSize)
	for i := 0; i < poolSize; i += 1 {
		conn, err := connect(ctx, address)
		if err != nil {
			return nil, err
		}
		conns[i] = conn
	}
	return &RoundRobinConnPool{
		Index:       0,
		Size:        poolSize,
		Connections: conns,
	}, nil
}

var retryPolicy = `{
	"methodConfig": [{
		"name": [
			{"service": "api.Cluster"}, 
			{"service": "api.Node"}, 
			{"service": "api.Clock"}
		],
		"waitForReady": true,

		"retryPolicy": {
			"MaxAttempts": 3,
			"InitialBackoff": "0.1s",
			"MaxBackoff": "2s",
			"BackoffMultiplier": 4.0,
			"RetryableStatusCodes": [ "UNAVAILABLE" ]
		}
	}]
}`

func connect(ctx context.Context, address string) (*grpc.ClientConn, error) {
	// keepaliveClientParams := keepalive.ClientParameters{
	// 	Time:                PING_RATE,
	// 	Timeout:             PING_TIMEOUT,
	// 	PermitWithoutStream: true,
	// }

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return grpc.DialContext(
		ctx,
		address,
		grpc.WithBlock(),
		grpc.WithInsecure(),
		// grpc.WithKeepaliveParams(keepaliveClientParams),
		grpc.WithDefaultServiceConfig(retryPolicy),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MAX_MSG_SIZE),
			grpc.MaxCallSendMsgSize(MAX_MSG_SIZE),
			grpc.WaitForReady(true),
		),
	)
}

func (r *RoundRobinConnPool) Conn() *grpc.ClientConn {
	newIdx := atomic.AddInt32(&r.Index, 1)
	return r.Connections[newIdx%int32(r.Size)]
}

func (r *RoundRobinConnPool) PoolSize() int {
	return r.Size
}

func (r *RoundRobinConnPool) Close() []error {
	errs := []error{}
	for _, conn := range r.Connections {
		err := conn.Close()
		errs = append(errs, err)
	}
	return errs
}

func (p *RoundRobinConnPool) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	return p.Conn().Invoke(ctx, method, args, reply, opts...)
}

func (p *RoundRobinConnPool) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return p.Conn().NewStream(ctx, desc, method, opts...)
}
