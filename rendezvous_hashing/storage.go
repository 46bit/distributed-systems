package rendezvous_hashing

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Entry struct {
	Key   string `yaml:"key"`
	Value string `yaml:"value"`
}

type Storage struct {
	BadgerDb *badger.DB
}

func NewStorage(dbPath string) (*Storage, error) {
	options := badger.DefaultOptions(dbPath)
	// FIXME: Make this configurable
	options.ValueLogFileSize = 256 << 20

	badgerDb, err := badger.Open(options)
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
		opt.PrefetchValues = false
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

func SetupBadgerStorageMetrics() {
	badgerExpvarCollector := collectors.NewExpvarCollector(map[string]*prometheus.Desc{
		"badger_v3_blocked_puts_total":   prometheus.NewDesc("badger_v3_blocked_puts_total", "Blocked Puts", nil, nil),
		"badger_v3_compactions_current":  prometheus.NewDesc("badger_v3_compactions_current", "Tables being compacted", nil, nil),
		"badger_v3_disk_reads_total":     prometheus.NewDesc("badger_v3_disk_reads_total", "Disk Reads", nil, nil),
		"badger_v3_disk_writes_total":    prometheus.NewDesc("badger_v3_disk_writes_total", "Disk Writes", nil, nil),
		"badger_v3_gets_total":           prometheus.NewDesc("badger_v3_gets_total", "Gets", nil, nil),
		"badger_v3_puts_total":           prometheus.NewDesc("badger_v3_puts_total", "Puts", nil, nil),
		"badger_v3_memtable_gets_total":  prometheus.NewDesc("badger_v3_memtable_gets_total", "Memtable gets", nil, nil),
		"badger_v3_lsm_size_bytes":       prometheus.NewDesc("badger_v3_lsm_size_bytes", "LSM Size in bytes", []string{"database"}, nil),
		"badger_v3_vlog_size_bytes":      prometheus.NewDesc("badger_v3_vlog_size_bytes", "Value Log Size in bytes", []string{"database"}, nil),
		"badger_v3_pending_writes_total": prometheus.NewDesc("badger_v3_pending_writes_total", "Pending Writes", []string{"database"}, nil),
		"badger_v3_read_bytes":           prometheus.NewDesc("badger_v3_read_bytes", "Read bytes", nil, nil),
		"badger_v3_written_bytes":        prometheus.NewDesc("badger_v3_written_bytes", "Written bytes", nil, nil),
		"badger_v3_lsm_bloom_hits_total": prometheus.NewDesc("badger_v3_lsm_bloom_hits_total", "LSM Bloom Hits", []string{"level"}, nil),
		"badger_v3_lsm_level_gets_total": prometheus.NewDesc("badger_v3_lsm_level_gets_total", "LSM Level Gets", []string{"level"}, nil),
	})
	prometheus.MustRegister(badgerExpvarCollector)
}
