package entry

// Entry represents a Key-Value pair in the LSM tree.
type Entry struct {
	Key       []byte
	Value     []byte
	Tombstone bool
}

func (entry Entry) Size() int {
	return len(entry.Key) + len(entry.Value) + 1 // +1 for the Tombstone byte
}
