package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	. "github.com/46bit/distributed_systems/rendezvous_hashing"
	"github.com/46bit/distributed_systems/rendezvous_hashing/api"
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

	// FIXME: Support persistent disk locations in config
	storage, err := NewStorage(nodeConfig.BadgerDbFolder)
	if err != nil {
		log.Fatal(fmt.Errorf("error initialising db: %w", err))
	}
	if nodeConfig.LocalMetricsAddress == "" {
		log.Println("Not starting metrics server because LocalMetricsAddress not configured")
	} else {
		SetupBadgerStorageMetrics()
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			// FIXME: Stop using fatal, everywhere. Shutdown badger gracefully!
			log.Fatal(http.ListenAndServe(nodeConfig.LocalMetricsAddress, nil))
		}()
	}

	clockServer, err := NewClockServer(nodeConfig.ClockEpochFilePath)
	if err != nil {
		log.Fatal(err)
	}

	cluster := NewCluster(clusterConfig)
	nodeServer := NewNodeServer(nodeConfig.Id, storage)
	clusterServer := NewClusterServer(cluster)

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(64<<20),
		grpc.MaxSendMsgSize(64<<20),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)
	api.RegisterClockServer(grpcServer, clockServer)
	api.RegisterNodeServer(grpcServer, nodeServer)
	api.RegisterClusterServer(grpcServer, clusterServer)
	grpc_prometheus.Register(grpcServer)

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
	if err := grpcServer.Serve(c); err != nil {
		log.Fatal(err)
	}
}
