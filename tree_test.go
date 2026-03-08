package lmstree

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "lsmtree-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestLSMTree_PutGet(t *testing.T) {
	tree, err := Open(DefaultOptions(tempDir(t)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tree.Close()

	if err := tree.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	val, ok := tree.Get([]byte("hello"))
	if !ok {
		t.Fatal("Get: key not found")
	}
	if !bytes.Equal(val, []byte("world")) {
		t.Fatalf("Get: got %q, want %q", val, "world")
	}
}

func TestLSMTree_GetMissing(t *testing.T) {
	tree, err := Open(DefaultOptions(tempDir(t)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tree.Close()

	_, ok := tree.Get([]byte("missing"))
	if ok {
		t.Fatal("Get: expected not found")
	}
}

func TestLSMTree_Overwrite(t *testing.T) {
	tree, err := Open(DefaultOptions(tempDir(t)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tree.Close()

	tree.Put([]byte("k"), []byte("v1"))
	tree.Put([]byte("k"), []byte("v2"))

	val, ok := tree.Get([]byte("k"))
	if !ok {
		t.Fatal("Get: key not found")
	}
	if !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("Get: got %q, want %q", val, "v2")
	}
}

func TestLSMTree_Delete(t *testing.T) {
	tree, err := Open(DefaultOptions(tempDir(t)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tree.Close()

	tree.Put([]byte("key"), []byte("value"))
	tree.Delete([]byte("key"))

	_, ok := tree.Get([]byte("key"))
	if ok {
		t.Fatal("Get: expected key to be deleted")
	}
}

func TestLSMTree_FlushToSSTable(t *testing.T) {
	opts := DefaultOptions(tempDir(t))
	opts.MemTableSize = 1 // flush after every Put
	opts.BlockSize = 64

	tree, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tree.Close()

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		val := []byte(fmt.Sprintf("val%02d", i))
		if err := tree.Put(key, val); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	// All keys must still be readable after multiple flushes.
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		want := []byte(fmt.Sprintf("val%02d", i))
		got, ok := tree.Get(key)
		if !ok {
			t.Fatalf("Get %s: not found", key)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get %s: got %q, want %q", key, got, want)
		}
	}
}

func TestLSMTree_CompactionDeleteShadowing(t *testing.T) {
	opts := DefaultOptions(tempDir(t))
	opts.MemTableSize = 1    // force flush on every Put/Delete
	opts.L0CompactThresh = 2 // compact after 2 L0 files
	opts.BlockSize = 64

	tree, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tree.Close()

	tree.Put([]byte("a"), []byte("1"))
	tree.Put([]byte("b"), []byte("2"))
	tree.Delete([]byte("a")) // tombstone should shadow the earlier value
	tree.Put([]byte("c"), []byte("3"))

	_, ok := tree.Get([]byte("a"))
	if ok {
		t.Fatal("Get 'a': expected deleted, got found")
	}

	val, ok := tree.Get([]byte("b"))
	if !ok || !bytes.Equal(val, []byte("2")) {
		t.Fatalf("Get 'b': got (%q, %v), want (\"2\", true)", val, ok)
	}

	val, ok = tree.Get([]byte("c"))
	if !ok || !bytes.Equal(val, []byte("3")) {
		t.Fatalf("Get 'c': got (%q, %v), want (\"3\", true)", val, ok)
	}
}
