package sstable

import (
	"bytes"
	"testing"

	"github.com/maksymus/lmstree/util"
)

func TestIndexBlock_EncodeDecode(t *testing.T) {
	original := &IndexBlock{
		entries: []*IndexEntry{
			{
				startKey: []byte("a"),
				endKey:   []byte("b"),
				block:    Block{offset: 0, length: 100},
			},
			{
				startKey: []byte("c"),
				endKey:   []byte("d"),
				block:    Block{offset: 100, length: 200},
			},
		},
	}

	data, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded := &IndexBlock{}
	if err := decoded.Decode(data); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if len(decoded.entries) != len(original.entries) {
		t.Fatalf("Expected %d entries, got %d", len(original.entries), len(decoded.entries))
	}

	for i, entry := range original.entries {
		decodedEntry := decoded.entries[i]
		if !bytes.Equal(entry.startKey, decodedEntry.startKey) ||
			!bytes.Equal(entry.endKey, decodedEntry.endKey) ||
			entry.block != decodedEntry.block {
			t.Errorf("Entry %d mismatch: expected %+v, got %+v", i, entry, decodedEntry)
		}
	}
}

func TestFooter_EncodeDecode(t *testing.T) {
	original := &Footer{
		meta:  Block{offset: 0, length: 256},
		index: Block{offset: 256, length: 512},
	}

	data, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded := &Footer{}
	if err := decoded.Decode(data); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.meta != original.meta {
		t.Errorf("Meta block mismatch: expected %+v, got %+v", original.meta, decoded.meta)
	}
	if decoded.index != original.index {
		t.Errorf("Index block mismatch: expected %+v, got %+v", original.index, decoded.index)
	}
}

func TestDataBlock_EncodeDecode(t *testing.T) {
	original := &DataBlock{
		entries: []*util.Entry{
			{Key: []byte("key1"), Value: []byte("value1"), Tombstone: false},
			{Key: []byte("key2"), Value: []byte("value2"), Tombstone: true},
		},
	}

	data, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded := &DataBlock{}
	if err := decoded.Decode(data); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if len(decoded.entries) != len(original.entries) {
		t.Fatalf("Expected %d entries, got %d", len(original.entries), len(decoded.entries))
	}

	for i, entry := range original.entries {
		decodedEntry := decoded.entries[i]
		if !bytes.Equal(entry.Key, decodedEntry.Key) ||
			!bytes.Equal(entry.Value, decodedEntry.Value) ||
			entry.Tombstone != decodedEntry.Tombstone {
			t.Errorf("Entry %d mismatch: expected %+v, got %+v", i, entry, decodedEntry)
		}
	}
}

func TestMetaBlock_EncodeDecode(t *testing.T) {
	meta := &MetaBlock{
		createdAt: 1625077800,
		level:     1,
	}

	data, err := meta.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded := &MetaBlock{}
	if err := decoded.Decode(data); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.createdAt != meta.createdAt {
		t.Errorf("createdAt mismatch: expected %d, got %d", meta.createdAt, decoded.createdAt)
	}
	if decoded.level != meta.level {
		t.Errorf("level mismatch: expected %d, got %d", meta.level, decoded.level)
	}
}

func TestDataBlock_Search(t *testing.T) {
	db := &DataBlock{
		entries: []*util.Entry{
			{Key: []byte("apple"), Value: []byte("fruit")},
			{Key: []byte("banana"), Value: []byte("fruit")},
			{Key: []byte("carrot"), Value: []byte("vegetable")},
		},
	}

	tests := []struct {
		key      []byte
		expected *util.Entry
		found    bool
	}{
		{key: []byte("apple"), expected: &util.Entry{Key: []byte("apple"), Value: []byte("fruit")}, found: true},
		{key: []byte("banana"), expected: &util.Entry{Key: []byte("banana"), Value: []byte("fruit")}, found: true},
		{key: []byte("carrot"), expected: &util.Entry{Key: []byte("carrot"), Value: []byte("vegetable")}, found: true},
		{key: []byte("date"), expected: nil, found: false},
	}
	for _, tt := range tests {
		entry, found := db.Search(tt.key)
		if found != tt.found {
			t.Errorf("Search(%s) found = %v; want %v", tt.key, found, tt.found)
		}
		if found && !bytes.Equal(entry.Key, tt.expected.Key) {
			t.Errorf("Search(%s) key = %s; want %s", tt.key, entry.Key, tt.expected.Key)
		}
		if found && !bytes.Equal(entry.Value, tt.expected.Value) {
			t.Errorf("Search(%s) value = %s; want %s", tt.key, entry.Value, tt.expected.Value)
		}
	}
}

func TestIndexBlock_Search(t *testing.T) {
	ib := &IndexBlock{
		entries: []*IndexEntry{
			{startKey: []byte("a"), endKey: []byte("c"), block: Block{offset: 0, length: 100}},
			{startKey: []byte("d"), endKey: []byte("f"), block: Block{offset: 100, length: 200}},
		},
	}

	tests := []struct {
		key      []byte
		expected Block
		found    bool
	}{
		{key: []byte("b"), expected: Block{offset: 0, length: 100}, found: true},
		{key: []byte("e"), expected: Block{offset: 100, length: 200}, found: true},
		{key: []byte("g"), expected: Block{}, found: false},
	}
	for _, tt := range tests {
		block, found := ib.Search(tt.key)
		if found != tt.found {
			t.Errorf("Search(%s) found = %v; want %v", tt.key, found, tt.found)
		}
		if found && block != tt.expected {
			t.Errorf("Search(%s) block = %+v; want %+v", tt.key, block, tt.expected)
		}
	}
}

func Test_Build(t *testing.T) {
	entries := []*util.Entry{
		{Key: []byte("apple"), Value: []byte("fruit")},
		{Key: []byte("banana"), Value: []byte("fruit")},
		{Key: []byte("carrot"), Value: []byte("vegetable")},
		{Key: []byte("date"), Value: []byte("fruit")},
		{Key: []byte("eggplant"), Value: []byte("vegetable")},
	}

	data, err := Build(entries, 50, 1)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if len(data) == 0 {
		t.Fatalf("Build returned empty data")
	}
}
