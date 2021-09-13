package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"

	. "github.com/46bit/distributed_systems/rendezvous_hashing"
	"github.com/46bit/distributed_systems/rendezvous_hashing/pb"
)

func main() {
	nodeConfigFilePath := os.Args[1]
	clusterConfigFilePath := os.Args[2]

	var nodeConfig NodeConfig
	yamlBytes, err := ioutil.ReadFile(nodeConfigFilePath)
	if err != nil {
		log.Fatal(fmt.Errorf("error reading node config file: %w", err))
	}
	if err = yaml.Unmarshal(yamlBytes, &nodeConfig); err != nil {
		log.Fatal(fmt.Errorf("error deserialising node config file: %w", err))
	}

	var clusterConfig ClusterConfig
	yamlBytes, err = ioutil.ReadFile(clusterConfigFilePath)
	if err != nil {
		log.Fatal(fmt.Errorf("error reading cluster config file: %w", err))
	}
	if err = yaml.Unmarshal(yamlBytes, &clusterConfig); err != nil {
		log.Fatal(fmt.Errorf("error deserialising cluster config file: %w", err))
	}
	clusterConfig.Initialise()

	cluster := NewCluster(&clusterConfig.Cluster)
	// FIXME: Support persistent disk locations in config
	storage, err := NewStorage(nodeConfig.Node.BadgerDbFolder)
	if err != nil {
		log.Fatal(fmt.Errorf("error initialising db: %w", err))
	}
	nodeServer := NewNodeServer(nodeConfig.Node.Id, storage, cluster)
	clusterServer := NewClusterServer(cluster)

	livenessSettings := LivenessSettings{
		GossipRegularity: 1 * time.Second,
		NodeTimeoutAfter: 2 * time.Second,
	}
	liveness := NewLiveness(nodeConfig.Node.Id, cluster, livenessSettings)
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

	c, err := net.Listen("tcp", nodeConfig.Node.LocalAddress)
	if err != nil {
		log.Fatal(err)
	}
	if err := s.Serve(c); err != nil {
		log.Fatal(err)
	}
}
