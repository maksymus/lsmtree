package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/maksymus/lmstree/util"
)

// DataBlock represents a block of key-value entries in the SSTable.
// Each DataBlock contains multiple entries sorted by key.
type DataBlock struct {
	entries []*util.Entry
}

func (db *DataBlock) Encode() ([]byte, error) {
	buffer := bytesBufPool.Get()
	defer bytesBufPool.Put(buffer)

	for _, entry := range db.entries {
		if entry == nil {
			continue
		}

		if err := errors.Join(
			binary.Write(buffer, binary.BigEndian, uint32(len(entry.Key))),   // Key length)
			binary.Write(buffer, binary.BigEndian, uint32(len(entry.Value))), // Value length
			binary.Write(buffer, binary.BigEndian, entry.Key),                // Key
			binary.Write(buffer, binary.BigEndian, entry.Value),              // Value
			binary.Write(buffer, binary.BigEndian, entry.Tombstone),          // Tombstone (deletion marker if any)
		); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func (db *DataBlock) Decode(data []byte) error {
	reader := bytes.NewReader(data)

	for {
		var keyLen uint32
		var valueLen uint32
		if err := errors.Join(
			binary.Read(reader, binary.BigEndian, &keyLen),
			binary.Read(reader, binary.BigEndian, &valueLen),
		); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		key := make([]byte, keyLen)
		value := make([]byte, valueLen)
		var tombstone bool
		if err := errors.Join(
			binary.Read(reader, binary.BigEndian, &key),
			binary.Read(reader, binary.BigEndian, &value),
			binary.Read(reader, binary.BigEndian, &tombstone),
		); err != nil {
			return err
		}

		entry := &util.Entry{
			Key:       key,
			Value:     value,
			Tombstone: tombstone,
		}
		db.entries = append(db.entries, entry)
	}
	return nil
}

func (db *DataBlock) Search(key []byte) (*util.Entry, bool) {
	start, end := 0, len(db.entries)-1
	for start <= end {
		mid := (start + end) / 2
		comp := bytes.Compare(db.entries[mid].Key, key)
		if comp == 0 {
			return db.entries[mid], true
		} else if comp < 0 {
			start = mid + 1
		} else {
			end = mid - 1
		}
	}
	return nil, false
}
