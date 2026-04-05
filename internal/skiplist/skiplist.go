package skiplist

import (
	"bytes"
	"math/rand"

	"github.com/maksymus/lmstree/entry"
)

// SkipList is a probabilistic data structure for sorted key-value storage.
type SkipList struct {
	head         *SkipListNode
	maxLevel     int
	currentLevel int
	length       int
	rand         *rand.Rand
}

// SkipListNode is a node in the skip list.
type SkipListNode struct {
	entry.Entry
	forward []*SkipListNode
	level   int
}

// NewSkipList creates a new skip list with the specified maximum level.
func NewSkipList(maxLevel int, rand *rand.Rand) *SkipList {
	return &SkipList{
		head:         &SkipListNode{forward: make([]*SkipListNode, maxLevel)},
		maxLevel:     maxLevel,
		currentLevel: 0,
		length:       0,
		rand:         rand,
	}
}

// Insert adds a new Key-Value pair to the skip list.
func (sl *SkipList) Insert(key []byte, value []byte) {
	node := &SkipListNode{
		Entry:   entry.Entry{Key: key, Value: value},
		forward: make([]*SkipListNode, sl.maxLevel),
		level:   0,
	}

	for i := 0; i < sl.maxLevel-1; i++ {
		if sl.rand.Intn(2) == 0 {
			node.level++
		} else {
			break
		}
	}

	if node.level > sl.currentLevel {
		sl.currentLevel = node.level
	}

	current := sl.head
	for i := sl.currentLevel; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].Key, key) < 0 {
			current = current.forward[i]
		}
		if i <= node.level {
			node.forward[i] = current.forward[i]
			current.forward[i] = node
		}
	}

	sl.length++
}

// Get retrieves the value associated with the given key.
func (sl *SkipList) Get(key []byte) ([]byte, bool) {
	current := sl.head
	for i := sl.currentLevel; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].Key, key) < 0 {
			current = current.forward[i]
		}
		if current.forward[i] != nil && bytes.Equal(current.forward[i].Key, key) {
			return current.forward[i].Value, true
		}
	}
	var zeroValue []byte
	return zeroValue, false
}

// Delete removes the key-value pair for the given key.
func (sl *SkipList) Delete(key []byte) bool {
	current := sl.head
	found := false

	for i := sl.currentLevel; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].Key, key) < 0 {
			current = current.forward[i]
		}
		if current.forward[i] != nil && bytes.Equal(current.forward[i].Key, key) {
			found = true
			current.forward[i] = current.forward[i].forward[i]
		}
	}

	if found {
		sl.length--
		return true
	}
	return false
}

// Update modifies the value for an existing key.
func (sl *SkipList) Update(key []byte, value []byte) bool {
	current := sl.head
	found := false

	for i := sl.currentLevel; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].Key, key) < 0 {
			current = current.forward[i]
		}
		if current.forward[i] != nil && bytes.Equal(current.forward[i].Key, key) {
			current.forward[i].Value = value
			found = true
		}
	}

	return found
}

// All returns all values in sorted key order.
func (sl *SkipList) All() [][]byte {
	values := make([][]byte, 0, sl.length)
	current := sl.head.forward[0]
	for current != nil {
		values = append(values, current.Value)
		current = current.forward[0]
	}
	return values
}

// Reset clears the skip list.
func (sl *SkipList) Reset() {
	sl.head = &SkipListNode{forward: make([]*SkipListNode, sl.maxLevel)}
	sl.currentLevel = 0
	sl.length = 0
}

// InsertEntry upserts an Entry into the skip list.
func (sl *SkipList) InsertEntry(e *entry.Entry) {
	update := make([]*SkipListNode, sl.maxLevel)
	current := sl.head
	for i := sl.currentLevel; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].Key, e.Key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	if next := update[0].forward[0]; next != nil && bytes.Equal(next.Key, e.Key) {
		next.Entry = *e
		return
	}

	node := &SkipListNode{
		Entry:   *e,
		forward: make([]*SkipListNode, sl.maxLevel),
		level:   0,
	}
	for i := 0; i < sl.maxLevel-1; i++ {
		if sl.rand.Intn(2) == 0 {
			node.level++
		} else {
			break
		}
	}
	if node.level > sl.currentLevel {
		for i := sl.currentLevel + 1; i <= node.level; i++ {
			update[i] = sl.head
		}
		sl.currentLevel = node.level
	}
	for i := 0; i <= node.level; i++ {
		node.forward[i] = update[i].forward[i]
		update[i].forward[i] = node
	}
	sl.length++
}

// GetEntry retrieves the Entry (including tombstone status) for the given key.
func (sl *SkipList) GetEntry(key []byte) (*entry.Entry, bool) {
	current := sl.head
	for i := sl.currentLevel; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].Key, key) < 0 {
			current = current.forward[i]
		}
		if current.forward[i] != nil && bytes.Equal(current.forward[i].Key, key) {
			e := current.forward[i].Entry
			return &e, true
		}
	}
	return nil, false
}

// Entries returns all entries in sorted key order, deduplicated.
func (sl *SkipList) Entries() []*entry.Entry {
	result := make([]*entry.Entry, 0, sl.length)
	current := sl.head.forward[0]
	var lastKey []byte
	for current != nil {
		if !bytes.Equal(current.Key, lastKey) {
			e := current.Entry
			result = append(result, &e)
			lastKey = current.Key
		}
		current = current.forward[0]
	}
	return result
}

// LowerBound finds the smallest key >= the given key.
func (sl *SkipList) LowerBound(key []byte) ([]byte, bool) {
	current := sl.head
	for i := sl.currentLevel; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].Key, key) < 0 {
			current = current.forward[i]
		}
		if current.forward[i] != nil && bytes.Equal(current.forward[i].Key, key) {
			return current.forward[i].Value, true
		}
	}
	if current.forward[0] != nil {
		return current.forward[0].Value, true
	}
	var zeroValue []byte
	return zeroValue, false
}
