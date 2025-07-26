package main

import (
	"bytes"
	"math/rand"
)

// SkipList represents a skip list data structure.
// It is a probabilistic data structure that allows for fast search, insert, and delete operations.
// A skip list consists of multiple levels of linked lists, where each level is a subset of the elements in the lower level.
// The top level has the fewest elements, and each subsequent level has more elements.
// The skip list uses randomization to determine the level of each new node, allowing for efficient traversal.
type SkipList[V any] struct {
	head         *SkipListNode[V] // head is the top-level header node of the skip list.
	maxLevel     int              // maxLevel is the maximum level of the skip list.
	currentLevel int              // currentLevel is the current level of the skip list.
	length       int              // length is the number of elements in the skip list.
	rand         *rand.Rand       // rand is a random number generator used to determine the level of new nodes.
}

// SkipListNode represents a node in the skip list.
type SkipListNode[V any] struct {
	Entry[V]
	forward []*SkipListNode[V] // forward is an array of pointers to the next nodes at each level.
	level   int                // level is the level of the node in the skip list.
}

// NewSkipList creates a new skip list with the specified maximum level and random number generator.
func NewSkipList[V any](maxLevel int, rand *rand.Rand) *SkipList[V] {
	return &SkipList[V]{
		head:         &SkipListNode[V]{forward: make([]*SkipListNode[V], maxLevel)},
		maxLevel:     maxLevel,
		currentLevel: 0,
		length:       0,
		rand:         rand,
	}
}

// Insert adds a new key-value pair to the skip list.
func (sl *SkipList[V]) Insert(key []byte, value V) {
	node := &SkipListNode[V]{
		Entry: Entry[V]{
			key:   key,
			value: value,
		},
		forward: make([]*SkipListNode[V], sl.maxLevel),
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
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		if i <= node.level {
			node.forward[i] = current.forward[i]
			current.forward[i] = node
		}
	}

	sl.length++
}

// Get retrieves the value associated with the given key in the skip list.
func (sl *SkipList[V]) Get(key []byte) (V, bool) {
	current := sl.head
	for i := sl.currentLevel; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		if current.forward[i] != nil && bytes.Equal(current.forward[i].key, key) {
			return current.forward[i].value, true
		}
	}
	var zeroValue V
	return zeroValue, false
}

// Delete removes the key-value pair associated with the given key from the skip list.
func (sl *SkipList[V]) Delete(key []byte) bool {
	current := sl.head
	found := false

	for i := sl.currentLevel; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		if current.forward[i] != nil && bytes.Equal(current.forward[i].key, key) {
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

// Update modifies the value associated with the given key in the skip list.
func (sl *SkipList[V]) Update(key []byte, value V) bool {
	current := sl.head
	found := false

	for i := sl.currentLevel; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		if current.forward[i] != nil && bytes.Equal(current.forward[i].key, key) {
			current.forward[i].value = value
			found = true
		}
	}

	return found
}

// All returns all values in the skip list as a slice.
func (sl *SkipList[V]) All() []V {
	values := make([]V, 0, sl.length)
	current := sl.head.forward[0]
	for current != nil {
		values = append(values, current.value)
		current = current.forward[0]
	}
	return values
}
