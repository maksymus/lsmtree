package sstable

import (
	"bytes"

	"github.com/maksymus/lmstree/entry"
)

// Iterator walks the entries of a single SSTable in ascending key order,
// fetching one data block at a time rather than materializing the whole file.
// Tombstones are returned; callers are responsible for interpreting them.
type Iterator struct {
	reader   *Reader
	start    []byte
	end      []byte
	blockIdx int            // next index entry to fetch
	entries  []*entry.Entry // entries of the data block currently in hand
	entryIdx int            // next entry within entries
	err      error
}

// Iterator returns an Iterator over entries with start <= key < end.
// A nil start or end leaves that side unbounded.
func (r *Reader) Iterator(start, end []byte) *Iterator {
	it := &Iterator{reader: r, start: start, end: end}

	// Skip index entries whose key range ends before start.
	if start != nil {
		for it.blockIdx < len(r.index.entries) &&
			bytes.Compare(r.index.entries[it.blockIdx].endKey, start) < 0 {
			it.blockIdx++
		}
	}
	return it
}

// Next returns the next entry in range, or false once the range is exhausted
// or a read fails. Check Err after a false return to distinguish the two.
func (it *Iterator) Next() (*entry.Entry, bool) {
	for {
		if it.entryIdx < len(it.entries) {
			e := it.entries[it.entryIdx]
			it.entryIdx++

			// The first block may start below start; skip its leading entries.
			if it.start != nil && bytes.Compare(e.Key, it.start) < 0 {
				continue
			}
			if it.end != nil && bytes.Compare(e.Key, it.end) >= 0 {
				it.exhaust()
				return nil, false
			}
			return e, true
		}

		if it.err != nil || it.blockIdx >= len(it.reader.index.entries) {
			return nil, false
		}

		ie := it.reader.index.entries[it.blockIdx]
		it.blockIdx++

		// Index entries are ordered by startKey, so once a block begins at or
		// past end no later block can contribute.
		if it.end != nil && bytes.Compare(ie.startKey, it.end) >= 0 {
			it.exhaust()
			return nil, false
		}

		block, err := it.reader.readDataBlock(ie.block)
		if err != nil {
			it.err = err
			return nil, false
		}
		it.entries, it.entryIdx = block.entries, 0
	}
}

// Err returns the first read error encountered, if any.
func (it *Iterator) Err() error { return it.err }

// exhaust marks the iterator as fully consumed.
func (it *Iterator) exhaust() {
	it.entries, it.entryIdx = nil, 0
	it.blockIdx = len(it.reader.index.entries)
}
