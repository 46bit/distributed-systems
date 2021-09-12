package rendezvous_hashing

import (
	"github.com/dgraph-io/badger/v3"
)

type Entry struct {
	Key   string `yaml:"key"`
	Value string `yaml:"value"`
}

type Storage struct {
	BadgerDb *badger.DB
}

func NewStorage(dbPath string) (*Storage, error) {
	badgerDb, err := badger.Open(badger.DefaultOptions(dbPath))
	if err != nil {
		return nil, err
	}
	return &Storage{BadgerDb: badgerDb}, nil
}

func (s *Storage) Get(key string) (*string, error) {
	var valuePointer *string
	err := s.BadgerDb.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(value []byte) error {
			valueString := string(value)
			valuePointer = &valueString
			return nil
		})
	})
	return valuePointer, err
}

func (s *Storage) Set(entry *Entry) error {
	return s.BadgerDb.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(entry.Key), []byte(entry.Value))
	})
}

func (s *Storage) Keys() ([]string, error) {
	keys := []string{}
	err := s.BadgerDb.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.PrefetchSize = 10
		iter := txn.NewIterator(opt)
		defer iter.Close()
		for iter.Rewind(); iter.Valid(); iter.Next() {
			keys = append(keys, string(iter.Item().Key()))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

func (s *Storage) Close() {
	s.BadgerDb.Close()
}
