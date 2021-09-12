package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

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

	var entry *Entry
	var err error
	for _, chosenReplica := range chosenReplicas {
		entry, err = readEntryFromNode(key, chosenReplica.Node)
		if err == nil && entry != nil {
			break
		}
	}

	if err != nil {
		// FIXME: Return all errors not just final one?
		return nil, fmt.Errorf("read failed from all replicas, last error was: %w", err)
	}
	return entry, err
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

	g := new(errgroup.Group)
	for _, chosenReplica := range chosenReplicas {
		node := chosenReplica.Node
		g.Go(func() error {
			return writeEntryToNode(entry, node)
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("error writing to all replicas: %w", err)
	}
	return nil
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
