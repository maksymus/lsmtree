package util

import (
	"bytes"
	"reflect"
	"sync"
)

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
	return elem
}

func (p *SyncPool[T]) Put(item T) {
	value := reflect.ValueOf(item)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		return
	}

	if p.resetFunc != nil {
		p.resetFunc(item)
	}
	p.pool.Put(item)
}

func NewBytesBufferPool() *BytesBufferPool {
	return &BytesBufferPool{
		SyncPool: *NewSyncPool(
			func() *bytes.Buffer {
				return new(bytes.Buffer)
			},
			func(buf *bytes.Buffer) {
				buf.Reset()
			}),
	}
}
