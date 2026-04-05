package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/maksymus/lmstree/entry"
	"github.com/maksymus/lmstree/internal/pool"
)

type WalFile interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
}

// WAL represents a Write-Ahead Log for the LSM tree.
type WAL struct {
	mutex   sync.Mutex
	file    WalFile
	dir     string
	path    string
	version string
	pool    *pool.BytesBufferPool
}

func Create(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	createdAt := time.Now()
	version := fmt.Sprintf("%s-%d", createdAt.Format("20060102150405"), createdAt.Nanosecond())
	path := fmt.Sprintf("%s/wal-%s.log", dir, version)

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file:    file,
		dir:     dir,
		path:    path,
		version: version,
		pool:    pool.NewBytesBufferPool(),
	}, nil
}

func Open(path string) (*WAL, error) {
	walFile, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: walFile,
		dir:  filepath.Dir(path),
		path: path,
		pool: pool.NewBytesBufferPool(),
	}, nil
}

func (w *WAL) Write(entries ...*entry.Entry) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.file == nil {
		return fmt.Errorf("WAL file is not open")
	}

	for i, e := range entries {
		if e == nil {
			return fmt.Errorf("entry at index %d is nil", i)
		}
		if len(e.Key) == 0 {
			return fmt.Errorf("entry at index %d has empty Key", i)
		}
		if len(e.Value) == 0 && !e.Tombstone {
			return fmt.Errorf("entry at index %d has empty Value", i)
		}
	}

	buffer := w.pool.Get()
	defer w.pool.Put(buffer)

	for _, e := range entries {
		keyLen, dataLen := len(e.Key), len(e.Value)
		if err := errors.Join(
			binary.Write(buffer, binary.BigEndian, uint32(keyLen)),
			binary.Write(buffer, binary.BigEndian, uint32(dataLen)),
			binary.Write(buffer, binary.BigEndian, e.Key),
			binary.Write(buffer, binary.BigEndian, e.Value),
			binary.Write(buffer, binary.BigEndian, e.Tombstone),
		); err != nil {
			return err
		}

		if buffer.Len() > 5*1024*1024 {
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

func (w *WAL) Read() ([]*entry.Entry, error) {
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

	var entries []*entry.Entry
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

		entries = append(entries, &entry.Entry{Key: key, Value: value, Tombstone: tombstone != 0})
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

func (w *WAL) Delete() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
		w.file = nil
	}

	return os.Remove(w.path)
}

func (w *WAL) CompareVersion(version string) int {
	walParts := strings.Split(w.version, "-")
	parts := strings.Split(version, "-")

	if len(walParts) != 2 || len(parts) != 2 {
		return 0
	}

	if walParts[0] != parts[0] {
		return strings.Compare(walParts[0], parts[0])
	}

	return strings.Compare(walParts[1], parts[1])
}

func VersionFromFileName(fileName string) (string, error) {
	if match, err := regexp.MatchString(`^wal-\d+-\d+\.log$`, fileName); err != nil || !match {
		return "", fmt.Errorf("invalid WAL file name: %s", fileName)
	}

	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(fileName, "wal-"), ".log"), "-")
	return fmt.Sprintf("%s-%s", parts[0], parts[1]), nil
}
