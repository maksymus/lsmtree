package sstable

import (
	"errors"
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

var bytesBufPool = util.NewBytesBufferPool()

type Block struct {
	offset uint64
	length uint64
}

func Build(entries []*util.Entry, blockSize int, level int) ([]byte, error) {
	sstableBuffer := bytesBufPool.Get()
	defer bytesBufPool.Put(sstableBuffer)

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

		blockHandle := Block{
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

		if _, err := sstableBuffer.Write(dataBlockBytes); err != nil {
			return nil, err
		}
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
		meta: Block{
			offset: offset,
			length: uint64(len(metaBlockBytes)),
		},
		index: Block{
			offset: offset + uint64(len(metaBlockBytes)),
			length: uint64(len(indexBlockBytes)),
		},
	}

	footerBytes, err := footer.Encode()
	if err != nil {
		return nil, err
	}

	// Assemble SSTable
	_, err1 := sstableBuffer.Write(metaBlockBytes)
	_, err2 := sstableBuffer.Write(indexBlockBytes)
	_, err3 := sstableBuffer.Write(footerBytes)

	if err := errors.Join(err1, err2, err3); err != nil {
		return nil, err
	}

	return sstableBuffer.Bytes(), nil
}
