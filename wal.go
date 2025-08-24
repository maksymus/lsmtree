package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type WalFile interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
}

// WAL represents a Write-Ahead Log (WAL) for the LSM tree.
// It is used to log changes before they are applied to the LSM tree.
// The WAL is stored in a file and is used to recover the state of the LSM tree in case of a crash.
type WAL struct {
	mutex   sync.Mutex // Mutex to ensure thread-safe access to the WAL file.
	file    WalFile    // File handle for the WAL file.
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

func WalExists(dir, version string) bool {
	path := fmt.Sprintf("%s/wal-%s.log", dir, version)
	_, err := os.Stat(path)
	return err == nil
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

	if w.file == nil {
		return fmt.Errorf("WAL file is not open")
	}

	// Validate entries
	for i, entry := range entries {
		if entry == nil {
			return fmt.Errorf("entry at index %d is nil", i)
		}
		if len(entry.Key) == 0 {
			return fmt.Errorf("entry at index %d has empty Key", i)
		}
		if len(entry.Value) == 0 {
			return fmt.Errorf("entry at index %d has empty Value", i)
		}
	}

	buffer := w.pool.Get()
	defer w.pool.Put(buffer)

	for _, entry := range entries {
		keyLen, dataLen := len(entry.Key), len(entry.Value)
		if err := errors.Join(
			binary.Write(buffer, binary.BigEndian, uint32(keyLen)),  // Key length
			binary.Write(buffer, binary.BigEndian, uint32(dataLen)), // Value length
			binary.Write(buffer, binary.BigEndian, entry.Key),       // Key
			binary.Write(buffer, binary.BigEndian, entry.Value),     // Value
			binary.Write(buffer, binary.BigEndian, entry.Tombstone), // Tombstone (deletion marker if any)
		); err != nil {
			return err
		}

		if buffer.Len() > 5*1024*1024 { // flush if buffer is larger than 5MB
			if _, err := w.file.Write(buffer.Bytes()); err != nil {
				return err
			}
			buffer.Reset()
		}
	}

	if buffer.Len() > 0 {
		if _, err := w.file.Write(buffer.Bytes()); err != nil {
			return err
		}
	}

	return nil
}

func Read(w *WAL) ([]*Entry, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.file == nil {
		return nil, fmt.Errorf("WAL file is not open")
	}

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	buffer := w.pool.Get()
	defer w.pool.Put(buffer)

	if _, err := buffer.ReadFrom(w.file); err != nil {
		return nil, err
	}

	var entries []*Entry
	reader := bytes.NewReader(buffer.Bytes())
	for {
		var keyLen uint32
		var dataLen uint32
		if err := errors.Join(
			binary.Read(reader, binary.BigEndian, &keyLen),
			binary.Read(reader, binary.BigEndian, &dataLen),
		); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		key := make([]byte, keyLen)
		value := make([]byte, dataLen)
		var tombstone uint8
		if err := errors.Join(
			binary.Read(reader, binary.BigEndian, &key),
			binary.Read(reader, binary.BigEndian, &value),
			binary.Read(reader, binary.BigEndian, &tombstone),
		); err != nil {
			return nil, err
		}

		entries = append(entries, &Entry{Key: key, Value: value, Tombstone: tombstone != 0})
	}

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
