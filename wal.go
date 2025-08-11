package main

import (
	"bytes"
	"fmt"
	"os"
	"sync"
)

// WAL represents a Write-Ahead Log (WAL) for the LSM tree.
// It is used to log changes before they are applied to the LSM tree.
// The WAL is stored in a file and is used to recover the state of the LSM tree in case of a crash.
type WAL struct {
	mutex   sync.Mutex // Mutex to ensure thread-safe access to the WAL file.
	file    *os.File   // File handle for the WAL file.
	dir     string     // Directory where the WAL file is stored.
	path    string     // Path to the WAL file.
	version string     // Version of the WAL file, used to differentiate between different versions of the WAL.
	pool    *BytesBufferPool
}

type BytesBufferPool struct {
	pool sync.Pool // Pool for reusing bytes.Buffer objects to reduce memory allocations.
}

func NewBytesBufferPool() *BytesBufferPool {
	return &BytesBufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

func (p *BytesBufferPool) Get() *bytes.Buffer {
	buf := p.pool.Get().(*bytes.Buffer)
	buf.Reset() // Reset the buffer to clear any previous data.
	return buf
}

func (p *BytesBufferPool) Put(buf *bytes.Buffer) {
	// Return the buffer to the pool for reuse.
	// This helps reduce memory allocations and improve performance.
	buf.Reset() // Reset the buffer before putting it back in the pool.
	p.pool.Put(buf)
}

func Create(dir, version string) (*WAL, error) {
	// Create the WAL directory if it doesn't exist.
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Construct the path for the WAL file.
	path := fmt.Sprintf("%s/wal-%s.log", dir, version)

	// Open the WAL file for appending.
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file:    file,
		dir:     dir,
		path:    path,
		version: version,
		pool:    NewBytesBufferPool(),
	}, nil
}

func Open(dir, version string) (*WAL, error) {
	// Construct the path for the WAL file.
	path := fmt.Sprintf("%s/wal-%s.log", dir, version)

	// Open the WAL file for reading and writing.
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file:    file,
		dir:     dir,
		path:    path,
		version: version,
		pool:    NewBytesBufferPool(),
	}, nil
}

func (w *WAL) Write(entries ...*Entry) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// TODO marshall the entries to bytes.Buffer
	if w.file == nil {
		return fmt.Errorf("WAL file is not open")
	}

	for _, entry := range entries {
		// Write the entry to the WAL file.
		if _, err := w.file.WriteString(fmt.Sprintf("%s\t%s\n", string(entry.key), entry.value)); err != nil {
			return err
		}
	}

	return nil
}

func Read(w *WAL) ([]*Entry, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	//TODO unmarshall the entries from bytes.Buffer
	var entries []*Entry
	//var key, value string
	//for {
	//	// Read each entry from the WAL file.
	//	if _, err := fmt.Fscanf(w.file, "%s\t%s\n", &key, &value); err != nil {
	//		break // EOF or error
	//	}
	//	entries = append(entries, &Entry{key: []byte(key), value: value})
	//}

	return entries, nil
}

func (w *WAL) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.file == nil {
		return fmt.Errorf("WAL file is not open")
	}

	if err := w.file.Close(); err != nil {
		return err
	}

	w.file = nil
	return nil
}
