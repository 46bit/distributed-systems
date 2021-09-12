package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"gopkg.in/yaml.v2"
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

	cluster := NewCluster(&config.Cluster)

	livenessSettings := LivenessSettings{
		GossipRegularity: 1 * time.Second,
		NodeTimeoutAfter: 2 * time.Second,
	}
	liveness := NewLiveness(nodeID, cluster, livenessSettings)
	go liveness.Run()

	storage := NewStorage()

	mux := http.NewServeMux()
	mux.Handle("/cluster/get", ReadHandler(cluster))
	mux.Handle("/cluster/set", WriteHandler(cluster))
	mux.Handle("/node/info", InfoHandler(storage, cluster))
	mux.Handle("/node/live", LivenessHandler(liveness))
	mux.Handle("/node/get", GetHandler(storage))
	mux.Handle("/node/set", SetHandler(storage))
	if err = http.ListenAndServe(":"+nodeID, mux); err != nil {
		log.Fatal(err)
	}
}
