package sstable

import (
	"errors"
	"time"

	"github.com/maksymus/lmstree/entry"
	"github.com/maksymus/lmstree/internal/bloom"
)

/*
SSTable on-disk format:

	+-------------------+
	| Data Block 1      |
	+-------------------+
	| Data Block ...    |
	+-------------------+
	| Data Block N      |
	+-------------------+
	| Meta Block        |
	+-------------------+
	| Index Block       |
	+-------------------+
	| Footer (32 bytes) |
	+-------------------+
*/

// Build constructs SSTable bytes from the given entries, block size, and level.
func Build(entries []*entry.Entry, blockSize int, level int) ([]byte, error) {
	sstableBuffer := bytesBufPool.Get()
	defer bytesBufPool.Put(sstableBuffer)

	// Split entries into data blocks.
	var dataBlocks []*DataBlock
	currentBlock := &DataBlock{}
	currentSize := 0

	for _, e := range entries {
		entrySize := e.Size()
		if currentSize+entrySize > blockSize && currentSize > 0 {
			dataBlocks = append(dataBlocks, currentBlock)
			currentBlock = &DataBlock{}
			currentSize = 0
		}
		currentBlock.entries = append(currentBlock.entries, e)
		currentSize += entrySize
	}
	if len(currentBlock.entries) > 0 {
		dataBlocks = append(dataBlocks, currentBlock)
	}

	// Write data blocks and build index.
	indexBlock := &IndexBlock{}
	var offset uint64

	for _, db := range dataBlocks {
		dataBlockBytes, err := db.Encode()
		if err != nil {
			return nil, err
		}

		indexBlock.entries = append(indexBlock.entries, &IndexEntry{
			startKey: db.entries[0].Key,
			endKey:   db.entries[len(db.entries)-1].Key,
			block:    Block{offset: offset, length: uint64(len(dataBlockBytes))},
		})

		offset += uint64(len(dataBlockBytes))

		if _, err := sstableBuffer.Write(dataBlockBytes); err != nil {
			return nil, err
		}
	}

	indexBlockBytes, err := indexBlock.Encode()
	if err != nil {
		return nil, err
	}

	// Build bloom filter over all keys.
	bf := bloom.NewBloomFilter(len(entries), 0.01)
	for _, e := range entries {
		bf.Add(e.Key)
	}
	metaBlock := &MetaBlock{
		createdAt: time.Now().Unix(),
		level:     level,
		bloom:     bf.Encode(),
	}

	metaBlockBytes, err := metaBlock.Encode()
	if err != nil {
		return nil, err
	}

	footer := &Footer{
		meta:  Block{offset: offset, length: uint64(len(metaBlockBytes))},
		index: Block{offset: offset + uint64(len(metaBlockBytes)), length: uint64(len(indexBlockBytes))},
	}

	footerBytes, err := footer.Encode()
	if err != nil {
		return nil, err
	}

	_, err1 := sstableBuffer.Write(metaBlockBytes)
	_, err2 := sstableBuffer.Write(indexBlockBytes)
	_, err3 := sstableBuffer.Write(footerBytes)

	if err := errors.Join(err1, err2, err3); err != nil {
		return nil, err
	}

	return sstableBuffer.Bytes(), nil
}
