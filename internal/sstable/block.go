package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/maksymus/lmstree/entry"
	"github.com/maksymus/lmstree/internal/pool"
)

var bytesBufPool = pool.NewBytesBufferPool()

// Block is an (offset, length) handle that points to a region within the SSTable file.
type Block struct {
	offset uint64
	length uint64
}

// ---- DataBlock ----

// DataBlock holds a sorted slice of entries encoded on disk.
type DataBlock struct {
	entries []*entry.Entry
}

func (db *DataBlock) Encode() ([]byte, error) {
	buffer := bytesBufPool.Get()
	defer bytesBufPool.Put(buffer)

	for _, e := range db.entries {
		if e == nil {
			continue
		}
		if err := errors.Join(
			binary.Write(buffer, binary.BigEndian, uint32(len(e.Key))),
			binary.Write(buffer, binary.BigEndian, uint32(len(e.Value))),
			binary.Write(buffer, binary.BigEndian, e.Key),
			binary.Write(buffer, binary.BigEndian, e.Value),
			binary.Write(buffer, binary.BigEndian, e.Tombstone),
		); err != nil {
			return nil, err
		}
	}
	return bytes.Clone(buffer.Bytes()), nil
}

func (db *DataBlock) Decode(data []byte) error {
	reader := bytes.NewReader(data)

	for {
		var keyLen, valueLen uint32
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

		db.entries = append(db.entries, &entry.Entry{Key: key, Value: value, Tombstone: tombstone})
	}
	return nil
}

func (db *DataBlock) Search(key []byte) (*entry.Entry, bool) {
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

// ---- IndexBlock ----

// IndexBlock maps key ranges to DataBlock locations for binary-search lookup.
type IndexBlock struct {
	entries []*IndexEntry
	block   Block
}

// IndexEntry maps a key range [startKey, endKey] to a data block.
type IndexEntry struct {
	startKey []byte
	endKey   []byte
	block    Block
}

func (ib *IndexBlock) Encode() ([]byte, error) {
	buffer := bytesBufPool.Get()
	defer bytesBufPool.Put(buffer)

	for _, e := range ib.entries {
		if e == nil {
			continue
		}
		if err := errors.Join(
			binary.Write(buffer, binary.BigEndian, uint32(len(e.startKey))),
			binary.Write(buffer, binary.BigEndian, uint32(len(e.endKey))),
			binary.Write(buffer, binary.BigEndian, e.startKey),
			binary.Write(buffer, binary.BigEndian, e.endKey),
			binary.Write(buffer, binary.BigEndian, e.block.offset),
			binary.Write(buffer, binary.BigEndian, e.block.length),
		); err != nil {
			return nil, err
		}
	}
	return bytes.Clone(buffer.Bytes()), nil
}

func (ib *IndexBlock) Decode(data []byte) error {
	reader := bytes.NewReader(data)

	for {
		var startKeyLen, endKeyLen uint32
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
		var blockOffset, blockLength uint64
		if err := errors.Join(
			binary.Read(reader, binary.BigEndian, &startKey),
			binary.Read(reader, binary.BigEndian, &endKey),
			binary.Read(reader, binary.BigEndian, &blockOffset),
			binary.Read(reader, binary.BigEndian, &blockLength),
		); err != nil {
			return err
		}

		ib.entries = append(ib.entries, &IndexEntry{
			startKey: startKey,
			endKey:   endKey,
			block:    Block{offset: blockOffset, length: blockLength},
		})
	}
	return nil
}

func (ib *IndexBlock) Search(key []byte) (Block, bool) {
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
	return Block{}, false
}

// ---- MetaBlock ----

// MetaBlock contains SSTable metadata: creation time, level, and bloom filter bytes.
type MetaBlock struct {
	createdAt int64
	level     int
	bloom     []byte
}

// Encode format: createdAt (8) | level (4) | bloomLen (4) | bloom (bloomLen bytes)
func (mb *MetaBlock) Encode() ([]byte, error) {
	buffer := bytesBufPool.Get()
	defer bytesBufPool.Put(buffer)

	if err := errors.Join(
		binary.Write(buffer, binary.BigEndian, mb.createdAt),
		binary.Write(buffer, binary.BigEndian, int32(mb.level)),
		binary.Write(buffer, binary.BigEndian, uint32(len(mb.bloom))),
	); err != nil {
		return nil, err
	}
	if _, err := buffer.Write(mb.bloom); err != nil {
		return nil, err
	}
	return bytes.Clone(buffer.Bytes()), nil
}

func (mb *MetaBlock) Decode(data []byte) error {
	reader := bytes.NewReader(data)

	var level int32
	var bloomLen uint32
	if err := errors.Join(
		binary.Read(reader, binary.BigEndian, &mb.createdAt),
		binary.Read(reader, binary.BigEndian, &level),
		binary.Read(reader, binary.BigEndian, &bloomLen),
	); err != nil {
		return err
	}
	mb.level = int(level)
	if bloomLen > 0 {
		mb.bloom = make([]byte, bloomLen)
		if _, err := reader.Read(mb.bloom); err != nil {
			return err
		}
	}
	return nil
}

// ---- Footer ----

// Footer is the fixed-size trailer of an SSTable file (4 × uint64 = 32 bytes).
type Footer struct {
	meta  Block
	index Block
}

func (f *Footer) Encode() ([]byte, error) {
	buffer := bytesBufPool.Get()
	defer bytesBufPool.Put(buffer)

	if err := errors.Join(
		binary.Write(buffer, binary.BigEndian, f.meta.offset),
		binary.Write(buffer, binary.BigEndian, f.meta.length),
		binary.Write(buffer, binary.BigEndian, f.index.offset),
		binary.Write(buffer, binary.BigEndian, f.index.length),
	); err != nil {
		return nil, err
	}
	return bytes.Clone(buffer.Bytes()), nil
}

func (f *Footer) Decode(data []byte) error {
	reader := bytes.NewReader(data)
	return errors.Join(
		binary.Read(reader, binary.BigEndian, &f.meta.offset),
		binary.Read(reader, binary.BigEndian, &f.meta.length),
		binary.Read(reader, binary.BigEndian, &f.index.offset),
		binary.Read(reader, binary.BigEndian, &f.index.length),
	)
}
