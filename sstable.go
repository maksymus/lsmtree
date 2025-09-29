package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

type DataBlock struct {
	pool    *BytesBufferPool
	entries []*Entry
}

type SSTable struct {
}

func (db *DataBlock) Encode() ([]byte, error) {
	buffer := db.pool.Get()
	defer db.pool.Put(buffer)

	for _, entry := range db.entries {
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
	buffer := db.pool.Get()
	defer db.pool.Put(buffer)

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

		entry := &Entry{
			Key:       key,
			Value:     value,
			Tombstone: tombstone,
		}
		db.entries = append(db.entries, entry)
	}
	return nil
}
