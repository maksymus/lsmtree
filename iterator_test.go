package lmstree

import (
	"bytes"
	"fmt"
	"testing"
)

// scanAll drains an Iterator into key/value slices.
func scanAll(t *testing.T, it *Iterator) ([]string, []string) {
	t.Helper()
	defer it.Close()

	var keys, values []string
	for {
		e, ok := it.Next()
		if !ok {
			break
		}
		if e.Tombstone {
			t.Fatalf("Scan yielded a tombstone for key %q", e.Key)
		}
		keys = append(keys, string(e.Key))
		values = append(values, string(e.Value))
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Iterator.Err: %v", err)
	}
	return keys, values
}

func wantKeys(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got keys %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("key %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestLSMTree_ScanMemTableOnly(t *testing.T) {
	tree, err := Open(DefaultOptions(tempDir(t)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tree.Close()

	// Insert out of order; Scan must return sorted order.
	for _, k := range []string{"c", "a", "e", "b", "d"} {
		if err := tree.Put([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	keys, values := scanAll(t, tree.Scan(nil, nil))
	wantKeys(t, keys, "a", "b", "c", "d", "e")
	for i, k := range keys {
		if values[i] != "v-"+k {
			t.Fatalf("value for %q: got %q, want %q", k, values[i], "v-"+k)
		}
	}
}

func TestLSMTree_ScanBounds(t *testing.T) {
	tree, err := Open(DefaultOptions(tempDir(t)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tree.Close()

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		tree.Put([]byte(k), []byte(k))
	}

	tests := []struct {
		name       string
		start, end []byte
		want       []string
	}{
		{"full", nil, nil, []string{"a", "b", "c", "d", "e"}},
		{"bounded", []byte("b"), []byte("d"), []string{"b", "c"}},
		{"start inclusive", []byte("d"), nil, []string{"d", "e"}},
		{"end exclusive", nil, []byte("c"), []string{"a", "b"}},
		{"empty", []byte("c"), []byte("c"), nil},
		{"beyond last", []byte("z"), nil, nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			keys, _ := scanAll(t, tree.Scan(tc.start, tc.end))
			wantKeys(t, keys, tc.want...)
		})
	}
}

func TestLSMTree_ScanAcrossMemTableAndSSTables(t *testing.T) {
	opts := DefaultOptions(tempDir(t))
	opts.MemTableSize = 1 // flush after every Put
	opts.BlockSize = 64

	tree, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tree.Close()

	const n = 30
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		val := []byte(fmt.Sprintf("val%02d", i))
		if err := tree.Put(key, val); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	keys, values := scanAll(t, tree.Scan(nil, nil))
	if len(keys) != n {
		t.Fatalf("Scan returned %d keys, want %d", len(keys), n)
	}
	for i := 0; i < n; i++ {
		if keys[i] != fmt.Sprintf("key%02d", i) {
			t.Fatalf("key %d: got %q, want key%02d", i, keys[i], i)
		}
		if values[i] != fmt.Sprintf("val%02d", i) {
			t.Fatalf("value %d: got %q, want val%02d", i, values[i], i)
		}
	}
}

func TestLSMTree_ScanNewestWins(t *testing.T) {
	opts := DefaultOptions(tempDir(t))
	opts.MemTableSize = 1 // each write lands in its own SSTable
	opts.BlockSize = 64

	tree, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tree.Close()

	tree.Put([]byte("k"), []byte("v1"))
	tree.Put([]byte("k"), []byte("v2"))
	tree.Put([]byte("k"), []byte("v3"))

	keys, values := scanAll(t, tree.Scan(nil, nil))
	wantKeys(t, keys, "k")
	if values[0] != "v3" {
		t.Fatalf("value for 'k': got %q, want %q", values[0], "v3")
	}
}

func TestLSMTree_ScanSkipsDeleted(t *testing.T) {
	opts := DefaultOptions(tempDir(t))
	opts.MemTableSize = 1
	opts.BlockSize = 64

	tree, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tree.Close()

	tree.Put([]byte("a"), []byte("1"))
	tree.Put([]byte("b"), []byte("2"))
	tree.Put([]byte("c"), []byte("3"))
	tree.Delete([]byte("b")) // tombstone in a newer SSTable than the value

	keys, _ := scanAll(t, tree.Scan(nil, nil))
	wantKeys(t, keys, "a", "c")
}

func TestLSMTree_ScanDeletedInMemTable(t *testing.T) {
	tree, err := Open(DefaultOptions(tempDir(t)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tree.Close()

	tree.Put([]byte("a"), []byte("1"))
	tree.Put([]byte("b"), []byte("2"))
	tree.Delete([]byte("a"))

	keys, _ := scanAll(t, tree.Scan(nil, nil))
	wantKeys(t, keys, "b")
}

func TestLSMTree_ScanAfterCompaction(t *testing.T) {
	opts := DefaultOptions(tempDir(t))
	opts.MemTableSize = 1
	opts.L0CompactThresh = 2
	opts.BlockSize = 64

	tree, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tree.Close()

	for i := 0; i < 12; i++ {
		key := []byte(fmt.Sprintf("k%02d", i))
		if err := tree.Put(key, []byte(fmt.Sprintf("v%02d", i))); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
	tree.Delete([]byte("k05"))

	keys, _ := scanAll(t, tree.Scan(nil, nil))
	if len(keys) != 11 {
		t.Fatalf("Scan returned %d keys %v, want 11", len(keys), keys)
	}
	for _, k := range keys {
		if k == "k05" {
			t.Fatal("Scan returned deleted key k05")
		}
	}
	for i := 1; i < len(keys); i++ {
		if keys[i-1] >= keys[i] {
			t.Fatalf("Scan out of order at %d: %q >= %q", i, keys[i-1], keys[i])
		}
	}
}

// An open Iterator pins its SSTables, so a compaction that deletes them while
// the scan is in flight must not break the scan.
func TestLSMTree_ScanSurvivesConcurrentCompaction(t *testing.T) {
	opts := DefaultOptions(tempDir(t))
	opts.MemTableSize = 1
	opts.L0CompactThresh = 2
	opts.BlockSize = 64

	tree, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tree.Close()

	const n = 8
	for i := 0; i < n; i++ {
		tree.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)))
	}

	it := tree.Scan(nil, nil)
	defer it.Close()

	// Consume one entry, then force more flushes and compactions underneath.
	first, ok := it.Next()
	if !ok {
		t.Fatal("Next: expected at least one entry")
	}
	if !bytes.Equal(first.Key, []byte("k00")) {
		t.Fatalf("first key: got %q, want k00", first.Key)
	}

	for i := n; i < n+8; i++ {
		tree.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)))
	}

	// The iterator reads the snapshot taken at Scan time: keys k00..k07 only.
	count := 1
	for {
		e, ok := it.Next()
		if !ok {
			break
		}
		if string(e.Key) >= fmt.Sprintf("k%02d", n) {
			t.Fatalf("Scan saw key %q written after the snapshot", e.Key)
		}
		count++
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Iterator.Err: %v", err)
	}
	if count != n {
		t.Fatalf("Scan returned %d keys, want %d", count, n)
	}
}

func TestLSMTree_ScanEmpty(t *testing.T) {
	tree, err := Open(DefaultOptions(tempDir(t)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tree.Close()

	keys, _ := scanAll(t, tree.Scan(nil, nil))
	if len(keys) != 0 {
		t.Fatalf("Scan on empty tree returned %v", keys)
	}
}

func TestLSMTree_IteratorCloseIdempotent(t *testing.T) {
	opts := DefaultOptions(tempDir(t))
	opts.MemTableSize = 1
	opts.BlockSize = 64

	tree, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tree.Close()

	tree.Put([]byte("a"), []byte("1"))
	tree.Put([]byte("b"), []byte("2"))

	it := tree.Scan(nil, nil)
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A second Close must not double-release the pinned SSTables.
	if err := it.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
