package sstable

import (
	"bytes"

	"github.com/maksymus/lmstree/entry"
	"github.com/maksymus/lmstree/internal/heap"
)

// Merge performs a k-way merge of sorted entry slices.
// For duplicate keys, last-write-wins (highest listIndex wins).
// Tombstones are dropped from the output.
func Merge(entries ...[]*entry.Entry) ([]*entry.Entry, error) {
	type heapItem struct {
		entry      *entry.Entry
		listIndex  int
		entryIndex int
	}

	h := heap.NewHeap[heapItem](func(a, b heapItem) bool {
		compare := bytes.Compare(a.entry.Key, b.entry.Key)
		if compare == 0 {
			return a.listIndex > b.listIndex
		}
		return compare < 0
	})

	for i, list := range entries {
		if len(list) > 0 {
			h.Push(heapItem{entry: list[0], listIndex: i, entryIndex: 0})
		}
	}

	result := make([]*entry.Entry, 0)
	var lastKey []byte

	for h.Len() > 0 {
		item, _ := h.Pop()
		currentEntry := item.entry
		listIndex := item.listIndex
		entryIndex := item.entryIndex + 1

		list := entries[listIndex]
		if entryIndex < len(list) {
			h.Push(heapItem{entry: list[entryIndex], listIndex: listIndex, entryIndex: entryIndex})
		}

		if bytes.Equal(lastKey, currentEntry.Key) {
			continue
		}
		lastKey = currentEntry.Key

		if !currentEntry.Tombstone {
			result = append(result, currentEntry)
		}
	}

	return result, nil
}
