package main

import (
	"bytes"
	"math/rand"
)

// Entry represents a Key-Value pair in the LSM tree.
type Entry struct {
	Key       []byte // Key is the unique identifier for the entry.
	Value     []byte // Value is the data associated with the Key.
	Tombstone bool   // Tombstone indicates whether the entry is a Tombstone (deleted).
}

// SkipList represents a skip list data structure.
// It is a probabilistic data structure that allows for fast search, insert, and delete operations.
// A skip list consists of multiple levels of linked lists, where each level is a subset of the elements in the lower level.
// The top level has the fewest elements, and each subsequent level has more elements.
// The skip list uses randomization to determine the level of each new node, allowing for efficient traversal.
type SkipList struct {
	head         *SkipListNode // head is the top-level header node of the skip list.
	maxLevel     int           // maxLevel is the maximum level of the skip list.
	currentLevel int           // currentLevel is the current level of the skip list.
	length       int           // length is the number of elements in the skip list.
	rand         *rand.Rand    // rand is a random number generator used to determine the level of new nodes.
}

// SkipListNode represents a node in the skip list.
type SkipListNode struct {
	Entry
	forward []*SkipListNode // forward is an array of pointers to the next nodes at each level.
	level   int             // level is the level of the node in the skip list.
}

// NewSkipList creates a new skip list with the specified maximum level and random number generator.
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
		Entry: Entry{
			Key:   key,
			Value: value,
		},
		forward: make([]*SkipListNode, sl.maxLevel),
		level:   0,
	}

	// Determine the level of the new node.
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

// Get retrieves the Value associated with the given Key in the skip list.
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

// Delete removes the Key-Value pair associated with the given Key from the skip list.
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

// Update modifies the Value associated with the given Key in the skip list.
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

// All returns all values in the skip list as a slice.
func (sl *SkipList) All() [][]byte {
	values := make([][]byte, 0, sl.length)
	current := sl.head.forward[0]
	for current != nil {
		values = append(values, current.Value)
		current = current.forward[0]
	}
	return values
}

// Reset clears the skip list, removing all elements and resetting its state.
func (sl *SkipList) Reset() {
	sl.head = &SkipListNode{forward: make([]*SkipListNode, sl.maxLevel)}
	sl.currentLevel = 0
	sl.length = 0
}

// LowerBound finds the smallest key in the skip list that is greater than or equal to the given key.
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
