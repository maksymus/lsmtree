package main

// Entry represents a Key-Value pair in the LSM tree.
type Entry struct {
	Key       []byte // Key is the unique identifier for the entry.
	Value     []byte // Value is the data associated with the Key.
	Tombstone bool   // Tombstone indicates whether the entry is a Tombstone (deleted).
}
