package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

type IndexBlock struct {
	entries []*IndexEntry
	block   Block
}

// IndexEntry represents an entry in the index block, mapping a key range to a data block.
type IndexEntry struct {
	startKey []byte
	endKey   []byte
	block    Block
}

func (ib *IndexBlock) Encode() ([]byte, error) {
	buffer := bytesBufPool.Get()
	defer bytesBufPool.Put(buffer)

	for _, entry := range ib.entries {
		if entry == nil {
			continue
		}

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
			block: Block{
				offset: blockOffset,
				length: blockLength,
			},
		}
		ib.entries = append(ib.entries, entry)
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
