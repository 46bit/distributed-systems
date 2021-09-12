package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"

	"gopkg.in/yaml.v2"
)

type Storage struct {
	sync.Mutex

	Data map[string]string
}

func NewStorage() *Storage {
	return &Storage{
		Data: map[string]string{},
	}
}

func (s *Storage) Get(key string) *string {
	s.Lock()
	defer s.Unlock()

	value, ok := s.Data[key]
	if !ok {
		return nil
	}
	return &value
}

func (s *Storage) Set(entry *Entry) {
	s.Lock()
	defer s.Unlock()

	s.Data[entry.Key] = entry.Value
}

type InfoReply struct {
	Keys        []string `yaml:"keys"`
	OnlineNodes []string `yaml:"online_nodes"`
}

func InfoHandler(s *Storage, c *Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		infoReply := InfoReply{
			Keys:        []string{},
			OnlineNodes: []string{},
		}
		s.Lock()
		for key := range s.Data {
			infoReply.Keys = append(infoReply.Keys, key)
		}
		s.Unlock()
		c.Lock()
		for nodeID, _ := range c.OnlineNodes {
			infoReply.OnlineNodes = append(infoReply.OnlineNodes, nodeID)
		}
		c.Unlock()

		replyBytes, err := yaml.Marshal(&infoReply)
		if err != nil {
			w.WriteHeader(500)
			log.Println(fmt.Errorf("error serialising reply into YAML: %w", err))
			return
		}
		if _, err = w.Write(replyBytes); err != nil {
			w.WriteHeader(500)
			log.Println(fmt.Errorf("error sending reply: %w", err))
			return
		}
	}
}

type GetMessage struct {
	Key string `yaml:"key"`
}

func GetHandler(s *Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(500)
			log.Println(fmt.Errorf("error reading from connection: %w", err))
			return
		}
		var getMessage GetMessage
		if err = yaml.Unmarshal(bodyBytes, &getMessage); err != nil {
			w.WriteHeader(500)
			log.Println(fmt.Errorf("error deserialising body: %w", err))
		}

		value := s.Get(getMessage.Key)
		if value == nil {
			w.WriteHeader(404)
			return
		}
		entry := Entry{Key: getMessage.Key, Value: *value}

		replyBytes, err := yaml.Marshal(&entry)
		if err != nil {
			w.WriteHeader(500)
			log.Println(fmt.Errorf("error serialising reply into YAML: %w", err))
			return
		}
		if _, err = w.Write(replyBytes); err != nil {
			w.WriteHeader(500)
			log.Println(fmt.Errorf("error sending reply: %w", err))
			return
		}
	}
}

type SetMessage struct {
	Entry Entry `yaml:"entry"`
}

func SetHandler(s *Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(500)
			log.Println(fmt.Errorf("error reading from connection: %w", err))
			return
		}
		var setMessage SetMessage
		if err = yaml.Unmarshal(bodyBytes, &setMessage); err != nil {
			w.WriteHeader(500)
			log.Println(fmt.Errorf("error deserialising body: %w", err))
		}

		s.Set(&setMessage.Entry)
	}
}
