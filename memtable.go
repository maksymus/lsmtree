package lmstree

import (
	"math/rand"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/maksymus/lmstree/util"
	wal "github.com/maksymus/lmstree/wal"
)

// MemTable represents an in-memory table with a skip list and a write-ahead log (WAL).
// It supports concurrent access and can be set to read-only mode.
// The MemTable uses a mutex to ensure thread-safe operations.
// It contains a skip list for efficient key-value storage and retrieval,
// and a WAL for durability and recovery in case of crashes.
type MemTable struct {
	mutex    sync.Mutex
	list     *util.SkipList
	wal      *wal.WAL
	dir      string
	readonly bool
}

// NewMemTable creates a new MemTable with the specified directory and skip list level.
func NewMemTable(dir string, level int) (*MemTable, error) {
	wal, err := wal.Create(dir)
	if err != nil {
		return nil, err
	}

	list := util.NewSkipList(level, rand.New(rand.NewSource(time.Now().Unix())))

	return &MemTable{
		list: list,
		wal:  wal,
		dir:  dir,
	}, nil
}

// Set adds a key-value pair to the MemTable.
func (m *MemTable) Set(key, value []byte) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.readonly {
		return nil
	}

	entry := &util.Entry{
		Key:   key,
		Value: value,
	}

	if err := m.wal.Write(entry); err != nil {
		return err
	}

	m.list.Insert(key, value)
	return nil
}

// Get retrieves the value associated with the given key from the MemTable.
func (m *MemTable) Get(key []byte) ([]byte, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.list.Get(key)
}

// Recover replays the WAL files to restore the MemTable state after a crash.
func (m *MemTable) Recover() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	dirs, err := os.ReadDir(m.dir)
	if err != nil {
		return err
	}

	// Filter WAL files
	var files []string
	for _, file := range dirs {
		if version, err := wal.VersionFromFileName(file.Name()); err == nil {
			if !file.IsDir() && m.wal.CompareVersion(version) < 0 {
				files = append(files, file.Name())
			}

		}
	}

	if len(files) == 0 {
		return nil
	}

	// Sort WAL files by version (assuming version is numeric and increasing)
	slices.Sort(files)

	// Replay WAL files in order
	for _, file := range files {
		wal, err := wal.Open(file)
		if err != nil {
			return err
		}

		entries, err := wal.Read()
		if err != nil {
			return err
		}

		// Replay entries into the skip list
		for _, entry := range entries {
			if err := m.wal.Write(entry); err != nil {
				return err
			}
			m.list.Insert(entry.Key, entry.Value)
		}

		if err := wal.Delete(); err != nil {
			return err
		}
	}

	return nil
}
