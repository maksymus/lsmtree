package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/maksymus/lmstree/entry"
)

// buildReader writes an SSTable holding entries and returns a Reader over it.
// blockSize is deliberately small in these tests so the table spans many blocks.
func buildReader(t *testing.T, entries []*entry.Entry, blockSize int) *Reader {
	t.Helper()

	data, err := Build(entries, blockSize, 0)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	path := filepath.Join(t.TempDir(), "test.sst")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func collect(t *testing.T, it *Iterator) []string {
	t.Helper()

	var keys []string
	for {
		e, ok := it.Next()
		if !ok {
			break
		}
		keys = append(keys, string(e.Key))
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Iterator.Err: %v", err)
	}
	return keys
}

func TestReaderIterator_Range(t *testing.T) {
	var entries []*entry.Entry
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		entries = append(entries, &entry.Entry{Key: key, Value: []byte("v")})
	}
	// 24 bytes per entry, so ~2 entries per block: the iterator must cross blocks.
	r := buildReader(t, entries, 48)

	tests := []struct {
		name       string
		start, end []byte
		wantFirst  string
		wantLast   string
		wantCount  int
	}{
		{"full", nil, nil, "key00", "key19", 20},
		{"bounded", []byte("key05"), []byte("key10"), "key05", "key09", 5},
		{"start only", []byte("key17"), nil, "key17", "key19", 3},
		{"end only", nil, []byte("key03"), "key00", "key02", 3},
		{"single key", []byte("key07"), []byte("key08"), "key07", "key07", 1},
		{"empty range", []byte("key07"), []byte("key07"), "", "", 0},
		{"below all", nil, []byte("key00"), "", "", 0},
		{"above all", []byte("key99"), nil, "", "", 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			keys := collect(t, r.Iterator(tc.start, tc.end))
			if len(keys) != tc.wantCount {
				t.Fatalf("got %d keys %v, want %d", len(keys), keys, tc.wantCount)
			}
			if tc.wantCount == 0 {
				return
			}
			if keys[0] != tc.wantFirst {
				t.Fatalf("first key: got %q, want %q", keys[0], tc.wantFirst)
			}
			if keys[len(keys)-1] != tc.wantLast {
				t.Fatalf("last key: got %q, want %q", keys[len(keys)-1], tc.wantLast)
			}
		})
	}
}

func TestReaderIterator_UnalignedBounds(t *testing.T) {
	entries := []*entry.Entry{
		{Key: []byte("b"), Value: []byte("1")},
		{Key: []byte("d"), Value: []byte("2")},
		{Key: []byte("f"), Value: []byte("3")},
		{Key: []byte("h"), Value: []byte("4")},
	}
	r := buildReader(t, entries, 16)

	// Bounds that fall between stored keys must still clip correctly.
	keys := collect(t, r.Iterator([]byte("c"), []byte("g")))
	want := []string{"d", "f"}
	if len(keys) != len(want) {
		t.Fatalf("got %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("keys[%d]: got %q, want %q", i, keys[i], want[i])
		}
	}
}

func TestReaderIterator_YieldsTombstones(t *testing.T) {
	entries := []*entry.Entry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte{}, Tombstone: true},
	}
	r := buildReader(t, entries, 4096)

	it := r.Iterator(nil, nil)
	it.Next() // "a"
	e, ok := it.Next()
	if !ok {
		t.Fatal("Next: expected tombstone entry for 'b'")
	}
	if !e.Tombstone {
		t.Fatalf("entry %q: expected tombstone", e.Key)
	}
}

func TestReaderIterator_MatchesEntries(t *testing.T) {
	var entries []*entry.Entry
	for i := 0; i < 50; i++ {
		entries = append(entries, &entry.Entry{
			Key:   []byte(fmt.Sprintf("k%03d", i)),
			Value: []byte(fmt.Sprintf("v%03d", i)),
		})
	}
	r := buildReader(t, entries, 64)

	all, err := r.Entries()
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	keys := collect(t, r.Iterator(nil, nil))
	if len(keys) != len(all) {
		t.Fatalf("iterator yielded %d keys, Entries returned %d", len(keys), len(all))
	}
	for i, e := range all {
		if keys[i] != string(e.Key) {
			t.Fatalf("keys[%d]: got %q, want %q", i, keys[i], e.Key)
		}
	}
}
