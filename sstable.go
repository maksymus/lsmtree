package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

/*
SSTable represents a Sorted String Table, which is an immutable data structure
used in LSM trees to store sorted key-value pairs on disk.

	+-------------------+
	| Data Block 1      |
	+-------------------+
	| Data Block ...    |
	+-------------------+
	| Data Block N      |
	+-------------------+
	| Meta Block		|
	+-------------------+
	| Index Block       |
	+-------------------+
	| Footer 			|
	+-------------------+
*/
type SSTable struct {
}

// DataBlock represents a block of key-value entries in the SSTable.
// Each DataBlock contains multiple entries sorted by key.
type DataBlock struct {
	pool    *BytesBufferPool
	entries []*Entry
}

// MetaBlock contains metadata about the SSTable, such as creation time and level in the LSM tree.
type MetaBlock struct {
	pool      *BytesBufferPool
	createdAt int64
	level     int
}

type IndexBlock struct {
	pool    *BytesBufferPool
	entries []*IndexEntry
	block   BlockHandle
}

type Footer struct {
	pool  *BytesBufferPool
	meta  BlockHandle
	index BlockHandle
}

type BlockHandle struct {
	offset uint64
	length uint64
}

// IndexEntry represents an entry in the index block, mapping a key range to a data block.
type IndexEntry struct {
	startKey []byte
	endKey   []byte
	block    BlockHandle
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

func (db *DataBlock) Search(key []byte) (*Entry, bool) {
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

func (ib *IndexBlock) Encode() ([]byte, error) {
	buffer := ib.pool.Get()
	defer ib.pool.Put(buffer)

	for _, entry := range ib.entries {
		if err := errors.Join(
			binary.Write(buffer, binary.BigEndian, uint32(len(entry.startKey))), // Start key length
			binary.Write(buffer, binary.BigEndian, uint32(len(entry.endKey))),   // End key length
			binary.Write(buffer, binary.BigEndian, entry.startKey),              // Start key
			binary.Write(buffer, binary.BigEndian, entry.endKey),                // End key
			binary.Write(buffer, binary.BigEndian, entry.block.offset),          // Block offset
			binary.Write(buffer, binary.BigEndian, entry.block.length),          // Block length
		); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func (ib *IndexBlock) Decode(data []byte) error {
	buffer := ib.pool.Get()
	defer ib.pool.Put(buffer)

	reader := bytes.NewReader(data)

	for {
		var startKeyLen uint32
		var endKeyLen uint32
		if err := errors.Join(
			binary.Read(reader, binary.BigEndian, &startKeyLen),
			binary.Read(reader, binary.BigEndian, &endKeyLen),
		); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		startKey := make([]byte, startKeyLen)
		endKey := make([]byte, endKeyLen)
		var blockOffset uint64
		var blockLength uint64
		if err := errors.Join(
			binary.Read(reader, binary.BigEndian, &startKey),
			binary.Read(reader, binary.BigEndian, &endKey),
			binary.Read(reader, binary.BigEndian, &blockOffset),
			binary.Read(reader, binary.BigEndian, &blockLength),
		); err != nil {
			return err
		}

		entry := &IndexEntry{
			startKey: startKey,
			endKey:   endKey,
			block: BlockHandle{
				offset: blockOffset,
				length: blockLength,
			},
		}
		ib.entries = append(ib.entries, entry)
	}
	return nil
}

func (ib *IndexBlock) Search(key []byte) (BlockHandle, bool) {
	start, end := 0, len(ib.entries)-1
	for start <= end {
		mid := (start + end) / 2
		if bytes.Compare(ib.entries[mid].startKey, key) <= 0 && bytes.Compare(ib.entries[mid].endKey, key) >= 0 {
			return ib.entries[mid].block, true
		} else if bytes.Compare(ib.entries[mid].startKey, key) < 0 {
			start = mid + 1
		} else {
			end = mid - 1
		}
	}
	return BlockHandle{}, false
}

func (f *Footer) Encode() ([]byte, error) {
	buffer := f.pool.Get()
	defer f.pool.Put(buffer)

	if err := errors.Join(
		binary.Write(buffer, binary.BigEndian, f.meta.offset),  // Meta block offset
		binary.Write(buffer, binary.BigEndian, f.meta.length),  // Meta block length
		binary.Write(buffer, binary.BigEndian, f.index.offset), // Index block offset
		binary.Write(buffer, binary.BigEndian, f.index.length), // Index block length
	); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (f *Footer) Decode(data []byte) error {
	buffer := f.pool.Get()
	defer f.pool.Put(buffer)

	reader := bytes.NewReader(data)

	if err := errors.Join(
		binary.Read(reader, binary.BigEndian, &f.meta.offset),
		binary.Read(reader, binary.BigEndian, &f.meta.length),
		binary.Read(reader, binary.BigEndian, &f.index.offset),
		binary.Read(reader, binary.BigEndian, &f.index.length),
	); err != nil {
		return err
	}
	return nil
}

func (mb *MetaBlock) Encode() ([]byte, error) {
	buffer := mb.pool.Get()
	defer mb.pool.Put(buffer)

	if err := errors.Join(
		binary.Write(buffer, binary.BigEndian, mb.createdAt),    // Creation timestamp
		binary.Write(buffer, binary.BigEndian, int32(mb.level)), // Level in LSM tree
	); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (mb *MetaBlock) Decode(data []byte) error {
	buffer := mb.pool.Get()
	defer mb.pool.Put(buffer)

	reader := bytes.NewReader(data)

	var level int32
	if err := errors.Join(
		binary.Read(reader, binary.BigEndian, &mb.createdAt),
		binary.Read(reader, binary.BigEndian, &level),
	); err != nil {
		return err
	}
	mb.level = int(level)
	return nil
}
