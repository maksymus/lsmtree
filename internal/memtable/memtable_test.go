package memtable

import (
	"bytes"
	"sync"
	"testing"

	"github.com/maksymus/lmstree/internal/wal"
)

func TestNewMemTable(t *testing.T) {
	mt := NewMemTable("/tmp", 5, &wal.NoopWAL{})
	if mt == nil {
		t.Fatal("expected non-nil MemTable")
	}

	if err := mt.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set on new MemTable: %v", err)
	}

	val, ok := mt.Get([]byte("k"))
	if !ok || !bytes.Equal(val, []byte("v")) {
		t.Fatalf("Get after Set: got (%v, %v), want (v, true)", val, ok)
	}
}

func TestMemTable_Set(t *testing.T) {
	mt := NewMemTable("/tmp", 5, &wal.NoopWAL{})

	if err := mt.Set([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Set returned unexpected error: %v", err)
	}

	val, ok := mt.Get([]byte("key1"))
	if !ok {
		t.Fatal("expected key1 to be found")
	}
	if !bytes.Equal(val, []byte("value1")) {
		t.Fatalf("expected value1, got %s", val)
	}
}

func TestMemTable_Set_Readonly(t *testing.T) {
	mt := NewMemTable("/tmp", 5, &wal.NoopWAL{})
	mt.readonly = true

	err := mt.Set([]byte("key1"), []byte("value1"))
	if err != ErrReadonly {
		t.Fatalf("expected ErrReadonly, got %v", err)
	}
}

func TestMemTable_Get_Found(t *testing.T) {
	mt := NewMemTable("/tmp", 5, &wal.NoopWAL{})
	mt.Set([]byte("hello"), []byte("world"))

	val, ok := mt.Get([]byte("hello"))
	if !ok {
		t.Fatal("expected key to be found")
	}
	if !bytes.Equal(val, []byte("world")) {
		t.Fatalf("expected world, got %s", val)
	}
}

func TestMemTable_Get_NotFound(t *testing.T) {
	mt := NewMemTable("/tmp", 5, &wal.NoopWAL{})
	_, ok := mt.Get([]byte("missing"))
	if ok {
		t.Fatal("expected key to not be found")
	}
}

func TestMemTable_ConcurrentSetGet(t *testing.T) {
	mt := NewMemTable("/tmp", 5, &wal.NoopWAL{})
	const goroutines = 50
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if err := mt.Set([]byte("key"), []byte("value")); err != nil {
					t.Errorf("goroutine %d: Set error: %v", id, err)
				}
			}
		}(g)
	}

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				mt.Get([]byte("key"))
			}
		}(g)
	}

	wg.Wait()
}
