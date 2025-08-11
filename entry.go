package main

// Entry represents a key-value pair in the LSM tree.
type Entry struct {
	key       []byte // Key is the unique identifier for the entry.
	value     []byte // Value is the data associated with the key.
	tombstone bool   // Tombstone indicates whether the entry is a tombstone (deleted).
}
