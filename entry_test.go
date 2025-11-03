package main

import (
	"bytes"
	"sync"
	"testing"
)

func TestBytesBufferPool_GetPut(t *testing.T) {
	pool := NewBytesBufferPool()

	// Get a buffer from the pool
	buf := pool.Get()
	if buf == nil {
		t.Errorf("Expected non-nil buffer from pool")
	}

	// Write something to the buffer
	buf.WriteString("Hello, World!")

	// Put the buffer back to the pool
	pool.Put(buf)

	// Get another buffer from the pool
	buf2 := pool.Get()
	if buf2 == nil {
		t.Errorf("Expected non-nil buffer from pool")
	}

	// Check if the buffer is reset
	if buf2.Len() != 0 {
		t.Errorf("Expected buffer to be reset, got length %d", buf2.Len())
	}
}

func TestBytesBufferPool_ConcurrentAccess(t *testing.T) {
	pool := NewBytesBufferPool()
	numGoroutines := 100
	numIterations := 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				buf := pool.Get()
				if buf == nil {
					t.Errorf("Expected non-nil buffer from pool")
				}
				buf.WriteString("Test")
				pool.Put(buf)
			}
		}()
	}
	wg.Wait()
}

func TestBytesBufferPool_ReuseBuffers(t *testing.T) {
	pool := NewBytesBufferPool()

	buf1 := pool.Get()
	pool.Put(buf1)

	buf2 := pool.Get()
	if buf1 != buf2 {
		t.Errorf("Expected to reuse the same buffer instance")
	}
}

func TestBytesBufferPool_MultiplePuts(t *testing.T) {
	pool := NewBytesBufferPool()

	buf := pool.Get()
	pool.Put(buf)
	pool.Put(buf) // Put the same buffer again

	buf2 := pool.Get()
	if buf != buf2 {
		t.Errorf("Expected to reuse the same buffer instance after multiple puts")
	}
}

func TestBytesBufferPool_EmptyPool(t *testing.T) {
	pool := NewBytesBufferPool()

	// Get multiple buffers without putting any back
	buf1 := pool.Get()
	buf2 := pool.Get()
	buf3 := pool.Get()

	if buf1 == nil || buf2 == nil || buf3 == nil {
		t.Errorf("Expected non-nil buffers from pool")
	}

	// Put them back
	pool.Put(buf1)
	pool.Put(buf2)
	pool.Put(buf3)
}

func TestBytesBufferPool_BufferContentAfterPut(t *testing.T) {
	pool := NewBytesBufferPool()

	buf := pool.Get()
	buf.WriteString("Some data")
	pool.Put(buf)

	buf2 := pool.Get()
	if buf2.Len() != 0 {
		t.Errorf("Expected buffer to be reset after put, got length %d", buf2.Len())
	}
}

func TestBytesBufferPool_NilPut(t *testing.T) {
	pool := NewBytesBufferPool()

	var buf *bytes.Buffer = nil
	pool.Put(buf) // Should not panic or add nil to the pool
	buf2 := pool.Get()
	if buf2 == nil {
		t.Errorf("Expected non-nil buffer from pool after putting nil")
	}
}
