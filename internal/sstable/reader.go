package sstable

import (
	"fmt"
	"os"

	"github.com/maksymus/lmstree/entry"
	"github.com/maksymus/lmstree/internal/bloom"
)

// footerSize is the fixed size in bytes of the SSTable footer (4 × uint64).
const footerSize = 32

// Reader provides read-only access to a single on-disk SSTable.
// Only the footer, index block, and bloom filter are loaded at open time.
// Data blocks are fetched on demand via ReadAt.
type Reader struct {
	f     *os.File
	size  int64
	index *IndexBlock
	bloom *bloom.BloomFilter
}

// OpenReader opens the SSTable at path and loads the footer, index, and bloom filter.
func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if info.Size() < footerSize {
		f.Close()
		return nil, fmt.Errorf("sstable %s too small (%d bytes)", path, info.Size())
	}
	r := &Reader{f: f, size: info.Size()}

	footerBuf := make([]byte, footerSize)
	if _, err := f.ReadAt(footerBuf, info.Size()-int64(footerSize)); err != nil {
		f.Close()
		return nil, err
	}
	footer := &Footer{}
	if err := footer.Decode(footerBuf); err != nil {
		f.Close()
		return nil, err
	}

	indexBuf := make([]byte, footer.index.length)
	if _, err := f.ReadAt(indexBuf, int64(footer.index.offset)); err != nil {
		f.Close()
		return nil, err
	}
	r.index = &IndexBlock{}
	if err := r.index.Decode(indexBuf); err != nil {
		f.Close()
		return nil, err
	}

	metaBuf := make([]byte, footer.meta.length)
	if _, err := f.ReadAt(metaBuf, int64(footer.meta.offset)); err == nil {
		meta := &MetaBlock{}
		if err := meta.Decode(metaBuf); err == nil && len(meta.bloom) > 0 {
			r.bloom, _ = bloom.Decode(meta.bloom)
		}
	}

	return r, nil
}

// Search looks up key in the SSTable. Returns the Entry (may be tombstone) and true if found.
func (r *Reader) Search(key []byte) (*entry.Entry, bool) {
	if r.bloom != nil && !r.bloom.Contains(key) {
		return nil, false
	}

	block, found := r.index.Search(key)
	if !found {
		return nil, false
	}

	buf := make([]byte, block.length)
	if _, err := r.f.ReadAt(buf, int64(block.offset)); err != nil {
		return nil, false
	}
	dataBlock := &DataBlock{}
	if err := dataBlock.Decode(buf); err != nil {
		return nil, false
	}
	return dataBlock.Search(key)
}

// Entries returns all entries in sorted key order, including tombstones.
func (r *Reader) Entries() ([]*entry.Entry, error) {
	var entries []*entry.Entry
	for _, ie := range r.index.entries {
		buf := make([]byte, ie.block.length)
		if _, err := r.f.ReadAt(buf, int64(ie.block.offset)); err != nil {
			return nil, err
		}
		dataBlock := &DataBlock{}
		if err := dataBlock.Decode(buf); err != nil {
			return nil, err
		}
		entries = append(entries, dataBlock.entries...)
	}
	return entries, nil
}

// Close releases the underlying file descriptor.
func (r *Reader) Close() error { return r.f.Close() }
