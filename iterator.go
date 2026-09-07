package lmstree

import (
	"bytes"

	"github.com/maksymus/lmstree/entry"
	"github.com/maksymus/lmstree/internal/heap"
	"github.com/maksymus/lmstree/internal/sstable"
)

// iterItem is one source's current entry, waiting its turn in the merge heap.
type iterItem struct {
	entry *entry.Entry
	// prio orders sources newest to oldest: the lower prio wins a key collision.
	prio int
	// advance yields the source's next entry, or false when it is exhausted.
	advance func() (*entry.Entry, bool)
}

// Iterator is a merged, ordered view over the MemTable and every SSTable level
// at the moment Scan was called. Entries are returned in ascending key order
// with duplicates resolved newest-first and tombstoned keys omitted.
//
// An Iterator pins the SSTables it reads, so a concurrent compaction cannot
// delete them out from under it. Callers must call Close to unpin them.
type Iterator struct {
	h       *heap.Heap[iterItem]
	held    []*sstableFile      // SSTables pinned for the life of the iterator
	sstIter []*sstable.Iterator // per-SSTable iterators, checked for read errors
	lastKey []byte
	hasLast bool
	closed  bool
}

// Scan returns an Iterator over all live keys in [start, end). A nil start
// begins at the first key; a nil end runs past the last, so Scan(nil, nil)
// walks the whole tree. The iterator reads a snapshot of the MemTable and the
// SSTable set as of this call; later writes are not reflected.
//
// The caller must Close the returned Iterator to release pinned SSTables.
func (t *LSMTree) Scan(start, end []byte) *Iterator {
	t.mu.RLock()
	defer t.mu.RUnlock()

	it := &Iterator{
		h: heap.NewHeap[iterItem](func(a, b iterItem) bool {
			if c := bytes.Compare(a.entry.Key, b.entry.Key); c != 0 {
				return c < 0
			}
			return a.prio < b.prio
		}),
	}

	// Sources are seeded newest to oldest, mirroring the search order in Get.
	prio := 0

	addSlice := func(entries []*entry.Entry) {
		i := 0
		it.seed(prio, func() (*entry.Entry, bool) {
			if i >= len(entries) {
				return nil, false
			}
			e := entries[i]
			i++
			return e, true
		})
		prio++
	}

	addSlice(t.memTable.Range(start, end))
	if t.immutable != nil {
		addSlice(t.immutable.Range(start, end))
	}

	for _, level := range t.levels {
		for _, sst := range level {
			sst.acquire()
			it.held = append(it.held, sst)

			sstIt := sst.reader.Iterator(start, end)
			it.sstIter = append(it.sstIter, sstIt)
			it.seed(prio, sstIt.Next)
			prio++
		}
	}

	return it
}

// seed pulls a source's first entry and pushes it onto the heap. A source that
// is empty from the start is simply never added.
func (it *Iterator) seed(prio int, advance func() (*entry.Entry, bool)) {
	if e, ok := advance(); ok {
		it.h.Push(iterItem{entry: e, prio: prio, advance: advance})
	}
}

// Next returns the next live entry in key order, or false once the range is
// exhausted. Check Err afterwards to tell exhaustion from a read failure.
func (it *Iterator) Next() (*entry.Entry, bool) {
	for it.h.Len() > 0 {
		item, _ := it.h.Pop()
		it.seed(item.prio, item.advance)

		// The heap yields the newest entry for a key first; every later copy of
		// that key is shadowed by it.
		if it.hasLast && bytes.Equal(item.entry.Key, it.lastKey) {
			continue
		}
		it.lastKey, it.hasLast = item.entry.Key, true

		if item.entry.Tombstone {
			continue
		}
		return item.entry, true
	}
	return nil, false
}

// Err returns the first read error hit by any underlying SSTable iterator.
func (it *Iterator) Err() error {
	for _, sstIt := range it.sstIter {
		if err := sstIt.Err(); err != nil {
			return err
		}
	}
	return nil
}

// Close releases the SSTables pinned by the iterator. It is safe to call more
// than once, and always returns nil.
func (it *Iterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	for _, sst := range it.held {
		sst.release()
	}
	it.held = nil
	return nil
}
