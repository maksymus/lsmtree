package sstable

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/maksymus/lmstree/entry"
)

func buildEntries(prefix string, n, valueLen int) []*entry.Entry {
	entries := make([]*entry.Entry, 0, n)
	for i := 0; i < n; i++ {
		entries = append(entries, &entry.Entry{
			Key:   []byte(fmt.Sprintf("%s%05d", prefix, i)),
			Value: bytes.Repeat([]byte(prefix), valueLen),
		})
	}
	return entries
}

// Build's result must not alias a buffer that gets recycled, or a later Build
// (or any other user of the shared block-encoding pool) overwrites it in place.
func TestBuild_ResultSurvivesLaterBuilds(t *testing.T) {
	first, err := Build(buildEntries("a", 40, 8), 64, 0)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	want := bytes.Clone(first)

	for i := 0; i < 5; i++ {
		if _, err := Build(buildEntries("b", 40, 8), 64, 0); err != nil {
			t.Fatalf("Build: %v", err)
		}
	}

	if !bytes.Equal(first, want) {
		t.Fatal("the first Build result was overwritten by a later Build")
	}
}

// Two SSTables built and held at the same time must stay independent.
func TestBuild_ConcurrentResultsIndependent(t *testing.T) {
	a, err := Build(buildEntries("a", 30, 4), 128, 0)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	b, err := Build(buildEntries("b", 30, 4), 128, 0)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if bytes.Equal(a, b) {
		t.Fatal("two different SSTables encoded identically")
	}
	if !bytes.Contains(a, []byte("a00000")) {
		t.Fatal("first SSTable no longer holds its own keys")
	}
	if !bytes.Contains(b, []byte("b00000")) {
		t.Fatal("second SSTable no longer holds its own keys")
	}
}

// The build buffer is sized from estimateSize; if it undershoots, every Build
// silently pays a regrow and full copy.
func TestBuild_SizeEstimateNeverUndershoots(t *testing.T) {
	tests := []struct {
		name      string
		n         int
		valueLen  int
		blockSize int
	}{
		{"tiny values", 500, 1, 64},
		{"small values", 200, 8, 256},
		{"large values", 100, 200, 4096},
		{"one entry", 1, 4, 4096},
		{"one entry per block", 50, 4, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries := buildEntries("k", tc.n, tc.valueLen)

			data, err := Build(entries, tc.blockSize, 0)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}

			// Re-derive the block split Build performs, to size the estimate.
			var dataBlocks []*DataBlock
			current := &DataBlock{}
			size := 0
			for _, e := range entries {
				if size+e.Size() > tc.blockSize && size > 0 {
					dataBlocks = append(dataBlocks, current)
					current = &DataBlock{}
					size = 0
				}
				current.entries = append(current.entries, e)
				size += e.Size()
			}
			if len(current.entries) > 0 {
				dataBlocks = append(dataBlocks, current)
			}

			if est := estimateSize(entries, dataBlocks); est < len(data) {
				t.Fatalf("estimateSize = %d, but the SSTable is %d bytes", est, len(data))
			}
		})
	}
}
