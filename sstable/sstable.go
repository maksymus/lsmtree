package sstable

import (
	"time"

	"github.com/maksymus/lmstree/util"
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

var pool = util.NewBytesBufferPool()

type BlockHandle struct {
	offset uint64
	length uint64
}

func Build(entries []*util.Entry, blockSize int, level int) ([]byte, error) {
	sstableBuffer := pool.Get()
	defer pool.Put(sstableBuffer)

	// Build Data Block
	dataBlocks := make([]*DataBlock, 0)
	currentBlock := &DataBlock{}
	currentSize := 0

	for _, entry := range entries {
		entrySize := entry.Size()
		if currentSize+entrySize > blockSize && currentSize > 0 {
			dataBlocks = append(dataBlocks, currentBlock)
			currentBlock = &DataBlock{}
			currentSize = 0
		}
		currentBlock.entries = append(currentBlock.entries, entry)
		currentSize += entrySize
	}
	if len(currentBlock.entries) > 0 {
		dataBlocks = append(dataBlocks, currentBlock)
	}

	// Build Index Block
	indexBlock := &IndexBlock{}
	var offset uint64 = 0

	for _, db := range dataBlocks {
		dataBlockBytes, err := db.Encode()
		if err != nil {
			return nil, err
		}

		blockHandle := BlockHandle{
			offset: offset,
			length: uint64(len(dataBlockBytes)),
		}

		startKey := db.entries[0].Key
		endKey := db.entries[len(db.entries)-1].Key

		indexEntry := &IndexEntry{
			startKey: startKey,
			endKey:   endKey,
			block:    blockHandle,
		}
		indexBlock.entries = append(indexBlock.entries, indexEntry)

		offset += uint64(len(dataBlockBytes))

		sstableBuffer.Write(dataBlockBytes)
	}

	indexBlockBytes, err := indexBlock.Encode()
	if err != nil {
		return nil, err
	}

	// Build Meta Block
	metaBlock := &MetaBlock{
		createdAt: time.Now().Unix(),
		level:     level,
	}

	metaBlockBytes, err := metaBlock.Encode()
	if err != nil {
		return nil, err
	}

	// Build Footer Block
	footer := &Footer{
		meta: BlockHandle{
			offset: offset,
			length: uint64(len(metaBlockBytes)),
		},
		index: BlockHandle{
			offset: offset + uint64(len(metaBlockBytes)),
			length: uint64(len(indexBlockBytes)),
		},
	}

	footerBytes, err := footer.Encode()
	if err != nil {
		return nil, err
	}

	// Assemble SSTable
	sstableBuffer.Write(metaBlockBytes)
	sstableBuffer.Write(indexBlockBytes)
	sstableBuffer.Write(footerBytes)

	return sstableBuffer.Bytes(), nil
}
