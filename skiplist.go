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
	// head is the top-level header node of the skip list.
	head *SkipListNode[V]
	// maxLevel is the maximum level of the skip list.
	maxLevel int
	// currentLevel is the current level of the skip list.
	currentLevel int
	// length is the number of elements in the skip list.
	length int
	// rand is a random number generator used to determine the level of new nodes.
	rand *rand.Rand
}

// SkipListNode represents a node in the skip list.
type SkipListNode[V any] struct {
	// key is the key of the node.
	key []byte
	// value is the value of the node.
	value V
	// forward is an array of pointers to the next nodes at each level.
	forward []*SkipListNode[V]
	// level is the level of the node in the skip list.
	level int
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
		key:     key,
		value:   value,
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
