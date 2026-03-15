package sstable

import (
	"fmt"
	"os"

	"github.com/maksymus/lmstree/util"
)

// footerSize is the fixed size in bytes of the SSTable footer (4 × uint64).
const footerSize = 32

// Reader provides read-only access to a single SSTable file loaded into memory.
type Reader struct {
	data  []byte
	path  string
	bloom *util.BloomFilter // decoded at open time; nil if absent
}

// OpenReader loads the SSTable at path into memory and returns a Reader.
// The bloom filter embedded in the MetaBlock is decoded once and cached for
// use in Search.
func OpenReader(path string) (*Reader, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < footerSize {
		return nil, fmt.Errorf("sstable %s too small (%d bytes)", path, len(data))
	}
	r := &Reader{data: data, path: path}

	// Decode the MetaBlock to get the bloom filter.
	footer := &Footer{}
	if err := footer.Decode(data[len(data)-footerSize:]); err == nil {
		meta := &MetaBlock{}
		if err := meta.Decode(data[footer.meta.offset : footer.meta.offset+footer.meta.length]); err == nil && len(meta.bloom) > 0 {
			r.bloom, _ = util.DecodeBloomFilter(meta.bloom)
		}
	}

	return r, nil
}

// Search looks up key in the SSTable using the index and data blocks.
// Returns the Entry (which may be a tombstone) and true if the key exists, or nil and false otherwise.
// Callers must check entry.Tombstone to distinguish live entries from deletions.
func (r *Reader) Search(key []byte) (*util.Entry, bool) {
	// Fast path: bloom filter rules out keys that are definitely absent.
	if r.bloom != nil && !r.bloom.Contains(key) {
		return nil, false
	}

	footer := &Footer{}
	if err := footer.Decode(r.data[len(r.data)-footerSize:]); err != nil {
		return nil, false
	}

	indexBlock := &IndexBlock{}
	if err := indexBlock.Decode(r.data[footer.index.offset : footer.index.offset+footer.index.length]); err != nil {
		return nil, false
	}

	block, found := indexBlock.Search(key)
	if !found {
		return nil, false
	}

	dataBlock := &DataBlock{}
	if err := dataBlock.Decode(r.data[block.offset : block.offset+block.length]); err != nil {
		return nil, false
	}

	return dataBlock.Search(key)
}

// Entries returns all entries stored in this SSTable in sorted key order,
// including tombstone entries.
func (r *Reader) Entries() ([]*util.Entry, error) {
	footer := &Footer{}
	if err := footer.Decode(r.data[len(r.data)-footerSize:]); err != nil {
		return nil, err
	}

	indexBlock := &IndexBlock{}
	if err := indexBlock.Decode(r.data[footer.index.offset : footer.index.offset+footer.index.length]); err != nil {
		return nil, err
	}

	var entries []*util.Entry
	for _, ie := range indexBlock.entries {
		dataBlock := &DataBlock{}
		if err := dataBlock.Decode(r.data[ie.block.offset : ie.block.offset+ie.block.length]); err != nil {
			return nil, err
		}
		entries = append(entries, dataBlock.entries...)
	}
	return entries, nil
}
