package main

// Entry represents a key-value pair in the LSM tree.
type Entry[T any] struct {
	key       []byte // Key is the unique identifier for the entry.
	value     T      // Value is the data associated with the key.
	tombstone bool   // Tombstone indicates whether the entry is a tombstone (deleted).
}
