package main

import (
	"bytes"
	"sync"
)

// Entry represents a Key-Value pair in the LSM tree.
type Entry struct {
	Key       []byte // Key is the unique identifier for the entry.
	Value     []byte // Value is the data associated with the Key.
	Tombstone bool   // Tombstone indicates whether the entry is a Tombstone (deleted).
}

type BytesBufferPool struct {
	pool sync.Pool // Pool for reusing bytes.Buffer objects to reduce memory allocations.
}

func NewBytesBufferPool() *BytesBufferPool {
	return &BytesBufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

func (p *BytesBufferPool) Get() *bytes.Buffer {
	buf := p.pool.Get().(*bytes.Buffer)
	buf.Reset() // Reset the buffer to clear any previous data.
	return buf
}

func (p *BytesBufferPool) Put(buf *bytes.Buffer) {
	// Return the buffer to the pool for reuse.
	// This helps reduce memory allocations and improve performance.
	buf.Reset() // Reset the buffer before putting it back in the pool.
	p.pool.Put(buf)
}
