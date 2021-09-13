package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	. "github.com/46bit/distributed_systems/rendezvous_hashing"
	"github.com/46bit/distributed_systems/rendezvous_hashing/pb"
)

func main() {
	nodeConfig, err := LoadNodeConfig(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	clusterConfig, err := LoadClusterConfig(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}

	cluster := NewCluster(clusterConfig)
	// FIXME: Support persistent disk locations in config
	storage, err := NewStorage(nodeConfig.BadgerDbFolder)
	if err != nil {
		log.Fatal(fmt.Errorf("error initialising db: %w", err))
	}
	nodeServer := NewNodeServer(nodeConfig.Id, storage, cluster)
	clusterServer := NewClusterServer(cluster)

	livenessSettings := LivenessSettings{
		GossipRegularity: 1 * time.Second,
		NodeTimeoutAfter: 2 * time.Second,
	}
	liveness := NewLiveness(nodeConfig.Id, cluster, livenessSettings)
	go liveness.Run()

	s := grpc.NewServer()
	pb.RegisterNodeServer(s, nodeServer)
	pb.RegisterClusterServer(s, clusterServer)

	exitSignals := make(chan os.Signal, 1)
	signal.Notify(exitSignals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-exitSignals
		storage.Close()
		os.Exit(0)
	}()

	c, err := net.Listen("tcp", nodeConfig.LocalAddress)
	if err != nil {
		log.Fatal(err)
	}
	if err := s.Serve(c); err != nil {
		log.Fatal(err)
	}
}
