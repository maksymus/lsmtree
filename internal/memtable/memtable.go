package memtable

import (
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/maksymus/lmstree/entry"
	"github.com/maksymus/lmstree/internal/skiplist"
	walPkg "github.com/maksymus/lmstree/internal/wal"
)

// ErrReadonly is returned when a write is attempted on a readonly MemTable.
var ErrReadonly = errors.New("memtable is readonly")

// WAL is the interface used by MemTable for write-ahead logging.
type WAL interface {
	Write(entries ...*entry.Entry) error
	CompareVersion(version string) int
}

// MemTable represents an in-memory table with a skip list and a write-ahead log.
type MemTable struct {
	mutex    sync.Mutex
	list     *skiplist.SkipList
	wal      WAL
	dir      string
	size     int64
	readonly bool
}

// NewMemTable creates a new MemTable with the specified directory, skip list level, and WAL.
func NewMemTable(dir string, level int, wal WAL) *MemTable {
	list := skiplist.NewSkipList(level, rand.New(rand.NewSource(time.Now().Unix())))
	return &MemTable{list: list, wal: wal, dir: dir}
}

// Set adds a key-value pair to the MemTable.
func (m *MemTable) Set(key, value []byte) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.readonly {
		return ErrReadonly
	}

	e := &entry.Entry{Key: key, Value: value}
	if err := m.wal.Write(e); err != nil {
		return err
	}
	m.list.InsertEntry(e)
	m.size += int64(len(key) + len(value))
	return nil
}

// Get retrieves the value for the given key. Does not expose tombstone status.
func (m *MemTable) Get(key []byte) ([]byte, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	e, ok := m.list.GetEntry(key)
	if !ok || e.Tombstone {
		return nil, false
	}
	return e.Value, true
}

// GetEntry retrieves the full Entry for the given key, including tombstone status.
func (m *MemTable) GetEntry(key []byte) (*entry.Entry, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.list.GetEntry(key)
}

// Delete marks the given key as deleted by inserting a tombstone entry.
func (m *MemTable) Delete(key []byte) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.readonly {
		return ErrReadonly
	}

	e := &entry.Entry{Key: key, Value: []byte{}, Tombstone: true}
	if err := m.wal.Write(e); err != nil {
		return err
	}
	m.list.InsertEntry(e)
	m.size += int64(len(key) + 1)
	return nil
}

// Size returns the approximate byte size of entries written to the MemTable.
func (m *MemTable) Size() int64 {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.size
}

// Entries returns all entries in sorted key order, deduplicated.
func (m *MemTable) Entries() []*entry.Entry {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.list.Entries()
}

// Recover replays older WAL files to restore the MemTable state after a crash.
func (m *MemTable) Recover() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	dirs, err := os.ReadDir(m.dir)
	if err != nil {
		return err
	}

	var files []string
	for _, file := range dirs {
		if version, err := walPkg.VersionFromFileName(file.Name()); err == nil {
			if !file.IsDir() && m.wal.CompareVersion(version) < 0 {
				files = append(files, file.Name())
			}
		}
	}

	if len(files) == 0 {
		return nil
	}

	slices.Sort(files)

	for _, file := range files {
		walFile, err := walPkg.Open(filepath.Join(m.dir, file))
		if err != nil {
			return err
		}

		entries, err := walFile.Read()
		if err != nil {
			return err
		}

		for _, e := range entries {
			if err := m.wal.Write(e); err != nil {
				return err
			}
			m.list.InsertEntry(e)
			if !e.Tombstone {
				m.size += int64(len(e.Key) + len(e.Value))
			}
		}

		if err := walFile.Delete(); err != nil {
			return err
		}
	}

	return nil
}
