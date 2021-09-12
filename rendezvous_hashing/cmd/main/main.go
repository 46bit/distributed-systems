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
	// FIXME: Take config better
	configFilePath := os.Args[1]
	nodeID := os.Args[2]

	var config Config
	yamlBytes, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		log.Fatal(fmt.Errorf("error reading config file: %w", err))
	}
	if err = yaml.Unmarshal(yamlBytes, &config); err != nil {
		log.Fatal(fmt.Errorf("error deserialising config file: %w", err))
	}
	config.Initialise()

	cluster := NewCluster(&config.Cluster)
	// FIXME: Support persistent disk locations in config
	storage, err := NewStorage("/tmp/rendezvous_hashing_badger_db_" + nodeID)
	if err != nil {
		log.Fatal(fmt.Errorf("error initialising db: %w", err))
	}
	nodeServer := NewNodeServer(nodeID, storage, cluster)
	clusterServer := NewClusterServer(cluster)

	livenessSettings := LivenessSettings{
		GossipRegularity: 1 * time.Second,
		NodeTimeoutAfter: 2 * time.Second,
	}
	liveness := NewLiveness(nodeID, cluster, livenessSettings)
	go liveness.Run()

	s := grpc.NewServer()
	pb.RegisterNodeServer(s, nodeServer)
	pb.RegisterClusterServer(s, clusterServer)

	exitSignals := make(chan os.Signal, 1)
	signal.Notify(exitSignals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-exitSignals
		storage.Close()
	}()

	c, err := net.Listen("tcp", ":"+nodeID)
	if err != nil {
		log.Fatal(err)
	}
	if err := s.Serve(c); err != nil {
		log.Fatal(err)
	}
}
