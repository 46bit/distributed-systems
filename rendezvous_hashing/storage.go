package rendezvous_hashing

import (
	"sync"
)

type Entry struct {
	Key   string `yaml:"key"`
	Value string `yaml:"value"`
}

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
