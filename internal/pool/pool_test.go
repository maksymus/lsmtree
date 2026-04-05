package pool

import (
	"bytes"
	"sync"
	"testing"
)

func TestBytesBufferPool_GetPut(t *testing.T) {
	p := NewBytesBufferPool()

	buf := p.Get()
	if buf == nil {
		t.Errorf("Expected non-nil buffer from pool")
	}
	buf.WriteString("Hello, World!")
	p.Put(buf)

	buf2 := p.Get()
	if buf2 == nil {
		t.Errorf("Expected non-nil buffer from pool")
	}
	if buf2.Len() != 0 {
		t.Errorf("Expected buffer to be reset, got length %d", buf2.Len())
	}
}

func TestBytesBufferPool_ConcurrentAccess(t *testing.T) {
	p := NewBytesBufferPool()
	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				buf := p.Get()
				if buf == nil {
					t.Errorf("Expected non-nil buffer from pool")
				}
				buf.WriteString("Test")
				p.Put(buf)
			}
		}()
	}
	wg.Wait()
}

func TestBytesBufferPool_ReuseBuffers(t *testing.T) {
	p := NewBytesBufferPool()
	buf1 := p.Get()
	p.Put(buf1)
	buf2 := p.Get()
	if buf1 != buf2 {
		t.Errorf("Expected to reuse the same buffer instance")
	}
}

func TestBytesBufferPool_MultiplePuts(t *testing.T) {
	p := NewBytesBufferPool()
	buf := p.Get()
	p.Put(buf)
	p.Put(buf)
	buf2 := p.Get()
	if buf != buf2 {
		t.Errorf("Expected to reuse the same buffer instance after multiple puts")
	}
}

func TestBytesBufferPool_EmptyPool(t *testing.T) {
	p := NewBytesBufferPool()
	buf1 := p.Get()
	buf2 := p.Get()
	buf3 := p.Get()
	if buf1 == nil || buf2 == nil || buf3 == nil {
		t.Errorf("Expected non-nil buffers from pool")
	}
	p.Put(buf1)
	p.Put(buf2)
	p.Put(buf3)
}

func TestBytesBufferPool_BufferContentAfterPut(t *testing.T) {
	p := NewBytesBufferPool()
	buf := p.Get()
	buf.WriteString("Some data")
	p.Put(buf)
	buf2 := p.Get()
	if buf2.Len() != 0 {
		t.Errorf("Expected buffer to be reset after put, got length %d", buf2.Len())
	}
}

func TestBytesBufferPool_NilPut(t *testing.T) {
	p := NewBytesBufferPool()
	var buf *bytes.Buffer = nil
	p.Put(buf)
	buf2 := p.Get()
	if buf2 == nil {
		t.Errorf("Expected non-nil buffer from pool after putting nil")
	}
}
