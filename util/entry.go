package util

import (
	"bytes"
	"reflect"
	"sync"
)

// Entry represents a Key-Value pair in the LSM tree.
type Entry struct {
	Key       []byte // Key is the unique identifier for the entry.
	Value     []byte // Value is the data associated with the Key.
	Tombstone bool   // Tombstone indicates whether the entry is a Tombstone (deleted).
}

func (entry Entry) Size() int {
	return len(entry.Key) + len(entry.Value) + 1 // +1 for the Tombstone byte
}

type SyncPool[T any] struct {
	pool      sync.Pool
	resetFunc func(T)
}

type BytesBufferPool struct {
	SyncPool[*bytes.Buffer]
}

func NewSyncPool[T any](newFunc func() T, resetFunc func(T)) *SyncPool[T] {
	return &SyncPool[T]{
		resetFunc: resetFunc,
		pool: sync.Pool{
			New: func() interface{} {
				return newFunc()
			},
		},
	}
}

func (p *SyncPool[T]) Get() T {
	elem := p.pool.Get().(T)
	if p.resetFunc != nil {
		p.resetFunc(elem)
	}
	return elem
}

func (p *SyncPool[T]) Put(item T) {
	value := reflect.ValueOf(item)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		return
	}

	p.pool.Put(item)
}

func NewBytesBufferPool() *BytesBufferPool {
	return &BytesBufferPool{
		SyncPool: *NewSyncPool(func() *bytes.Buffer {
			return new(bytes.Buffer)
		}, func(buf *bytes.Buffer) {
			buf.Reset()
		}),
	}
}
