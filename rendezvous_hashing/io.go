package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v2"
)

type Entry struct {
	Key   string `yaml:"key"`
	Value string `yaml:"value"`
}

func ReadHandler(c *Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Println(fmt.Errorf("error reading from connection: %w", err))
			return
		}
		var getMessage GetMessage
		if err = yaml.Unmarshal(bodyBytes, &getMessage); err != nil {
			log.Println(fmt.Errorf("error deserialising body: %w", err))
		}

		entry, err := Read(getMessage.Key, c)
		if err != nil {
			w.WriteHeader(500)
			log.Println(fmt.Errorf("error reading from cluster: %w", err))
			return
		}
		if entry == nil {
			w.WriteHeader(404)
			return
		}

		replyBytes, err := yaml.Marshal(&entry)
		if err != nil {
			w.WriteHeader(500)
			log.Println(fmt.Errorf("error serialising reply into YAML: %w", err))
			return
		}
		if _, err = w.Write(replyBytes); err != nil {
			log.Println(fmt.Errorf("error sending reply: %w", err))
			return
		}
	}
}

func Read(key string, cluster *Cluster) (*Entry, error) {
	chosenReplicas := cluster.FindNodesForKey(key)
	if len(chosenReplicas) == 0 {
		return nil, fmt.Errorf("no nodes found for key")
	}

	// Try to read from all replicas; succeed if more than half respond the same
	// FIXME: Implement conflict resolution so they don't have to respond the same
	entries := make(chan *Entry, len(chosenReplicas))
	g := new(errgroup.Group)
	for _, chosenReplica := range chosenReplicas {
		node := chosenReplica.Node
		g.Go(func() error {
			entry, err := readEntryFromNode(key, node)
			if entry != nil {
				entries <- entry
			}
			return err
		})
	}
	err := g.Wait()
	if err != nil {
		log.Println(fmt.Errorf("error while reading from all replicas: %w", err))
	}
	close(entries)

	// FIXME: Go may be a painful language sometimes but there is surely
	// something I can do to improve this code
	valuesSeen := map[string]int{}
	for entry := range entries {
		if _, ok := valuesSeen[entry.Value]; !ok {
			valuesSeen[entry.Value] = 1
		} else {
			valuesSeen[entry.Value] += 1
		}
	}
	if len(valuesSeen) == 0 {
		return nil, err
	}
	maxTimesSeen := 0
	maxSeenValue := ""
	for valueSeen, timesSeen := range valuesSeen {
		if timesSeen > maxTimesSeen {
			maxTimesSeen = timesSeen
			maxSeenValue = valueSeen
		}
	}
	if maxTimesSeen > len(chosenReplicas)/2 {
		return &Entry{
			Key:   key,
			Value: maxSeenValue,
		}, nil
	}
	return nil, fmt.Errorf("failed to read same value from enough replicas (only %d of %d)", maxTimesSeen, len(chosenReplicas))
}

func readEntryFromNode(key string, node *NodeDescription) (*Entry, error) {
	url := fmt.Sprintf("http://%s/node/get", node.RemoteAddress)
	messageBytes, err := yaml.Marshal(GetMessage{Key: key})
	if err != nil {
		return nil, fmt.Errorf("error serialising message into YAML: %w", err)
	}

	r, err := http.Post(url, "application/yaml", bytes.NewBuffer(messageBytes))
	if err != nil {
		return nil, fmt.Errorf("error posting message to node at '%s': %w", url, err)
	}
	if r.StatusCode == 404 {
		return nil, nil
	}
	if r.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected non-200 response code '%v'", r.StatusCode)
	}

	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading from connection: %w", err)
	}

	var entry Entry
	if err = yaml.Unmarshal(bodyBytes, &entry); err != nil {
		return nil, fmt.Errorf("error deserialising body: %w", err)
	}
	return &entry, nil
}

func WriteHandler(c *Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Println(fmt.Errorf("error reading from connection: %w", err))
			return
		}
		var setMessage SetMessage
		if err = yaml.Unmarshal(bodyBytes, &setMessage); err != nil {
			log.Println(fmt.Errorf("error deserialising body: %w", err))
		}

		if err = Write(setMessage.Entry, c); err != nil {
			w.WriteHeader(500)
			log.Println(fmt.Errorf("error writing to cluster: %w", err))
			return
		}
	}
}

func Write(entry Entry, cluster *Cluster) error {
	chosenReplicas := cluster.FindNodesForKey(entry.Key)
	if len(chosenReplicas) == 0 {
		return fmt.Errorf("no nodes found to accept key")
	}

	successfulWrites := uint64(0)
	g := new(errgroup.Group)
	for _, chosenReplica := range chosenReplicas {
		node := chosenReplica.Node
		g.Go(func() error {
			err := writeEntryToNode(entry, node)
			if err == nil {
				atomic.AddUint64(&successfulWrites, 1)
			}
			return err
		})
	}
	err := g.Wait()
	if successfulWrites > uint64(len(chosenReplicas)/2) {
		if err != nil {
			log.Println(err)
		}
		return nil
	}
	return err
}

func writeEntryToNode(entry Entry, node *NodeDescription) error {
	url := fmt.Sprintf("http://%s/node/set", node.RemoteAddress)
	messageBytes, err := yaml.Marshal(SetMessage{Entry: entry})
	if err != nil {
		return fmt.Errorf("error serialising message into YAML: %w", err)
	}

	r, err := http.Post(url, "application/yaml", bytes.NewBuffer(messageBytes))
	if err != nil {
		return fmt.Errorf("error posting message to node at '%s': %w", url, err)
	}
	if r.StatusCode != 200 {
		return fmt.Errorf("unexpected non-200 response code '%v'", r.StatusCode)
	}
	return nil
}
