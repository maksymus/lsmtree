package sstable

import (
	"bytes"
	"testing"

	"github.com/maksymus/lmstree/entry"
)

func TestIndexBlock_EncodeDecode(t *testing.T) {
	original := &IndexBlock{
		entries: []*IndexEntry{
			{startKey: []byte("a"), endKey: []byte("b"), block: Block{offset: 0, length: 100}},
			{startKey: []byte("c"), endKey: []byte("d"), block: Block{offset: 100, length: 200}},
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

	for i, e := range original.entries {
		de := decoded.entries[i]
		if !bytes.Equal(e.startKey, de.startKey) || !bytes.Equal(e.endKey, de.endKey) || e.block != de.block {
			t.Errorf("Entry %d mismatch: expected %+v, got %+v", i, e, de)
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
		entries: []*entry.Entry{
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

	for i, e := range original.entries {
		de := decoded.entries[i]
		if !bytes.Equal(e.Key, de.Key) || !bytes.Equal(e.Value, de.Value) || e.Tombstone != de.Tombstone {
			t.Errorf("Entry %d mismatch: expected %+v, got %+v", i, e, de)
		}
	}
}

func TestMetaBlock_EncodeDecode(t *testing.T) {
	meta := &MetaBlock{createdAt: 1625077800, level: 1}

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
		entries: []*entry.Entry{
			{Key: []byte("apple"), Value: []byte("fruit")},
			{Key: []byte("banana"), Value: []byte("fruit")},
			{Key: []byte("carrot"), Value: []byte("vegetable")},
		},
	}

	tests := []struct {
		key      []byte
		expected *entry.Entry
		found    bool
	}{
		{[]byte("apple"), &entry.Entry{Key: []byte("apple"), Value: []byte("fruit")}, true},
		{[]byte("banana"), &entry.Entry{Key: []byte("banana"), Value: []byte("fruit")}, true},
		{[]byte("carrot"), &entry.Entry{Key: []byte("carrot"), Value: []byte("vegetable")}, true},
		{[]byte("date"), nil, false},
	}

	for _, tt := range tests {
		e, found := db.Search(tt.key)
		if found != tt.found {
			t.Errorf("Search(%s) found = %v; want %v", tt.key, found, tt.found)
		}
		if found && !bytes.Equal(e.Key, tt.expected.Key) {
			t.Errorf("Search(%s) key = %s; want %s", tt.key, e.Key, tt.expected.Key)
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
		{[]byte("b"), Block{offset: 0, length: 100}, true},
		{[]byte("e"), Block{offset: 100, length: 200}, true},
		{[]byte("g"), Block{}, false},
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
	entries := []*entry.Entry{
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
		t.Fatal("Build returned empty data")
	}
}

func Test_Merge(t *testing.T) {
	entries1 := []*entry.Entry{
		{Key: []byte("apple"), Value: []byte("fruit")},
		{Key: []byte("fig"), Value: []byte("fruit")},
		{Key: []byte("grape"), Value: []byte("fruit")},
	}
	entries2 := []*entry.Entry{
		{Key: []byte("banana"), Value: []byte("fruit")},
		{Key: []byte("carrot"), Value: []byte("vegetable")},
		{Key: []byte("date"), Value: []byte("fruit")},
		{Key: []byte("eggplant"), Value: []byte("vegetable")},
	}

	mergedData, err := Merge(entries1, entries2)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	expected := []*entry.Entry{
		{Key: []byte("apple"), Value: []byte("fruit")},
		{Key: []byte("banana"), Value: []byte("fruit")},
		{Key: []byte("carrot"), Value: []byte("vegetable")},
		{Key: []byte("date"), Value: []byte("fruit")},
		{Key: []byte("eggplant"), Value: []byte("vegetable")},
		{Key: []byte("fig"), Value: []byte("fruit")},
		{Key: []byte("grape"), Value: []byte("fruit")},
	}

	if len(mergedData) != len(expected) {
		t.Fatalf("Expected %d entries, got %d", len(expected), len(mergedData))
	}

	for i, e := range mergedData {
		if !bytes.Equal(e.Key, expected[i].Key) || !bytes.Equal(e.Value, expected[i].Value) {
			t.Errorf("Entry %d mismatch: expected %+v, got %+v", i, expected[i], e)
		}
	}
}

func Test_MergeDuplicateKeys(t *testing.T) {
	entries1 := []*entry.Entry{
		{Key: []byte("apple"), Value: []byte("apple1")},
		{Key: []byte("banana"), Value: []byte("banana1")},
		{Key: []byte("mango"), Value: []byte("mango1")},
	}
	entries2 := []*entry.Entry{
		{Key: []byte("banana"), Value: []byte("banana2")},
		{Key: []byte("carrot"), Value: []byte("carrot2")},
	}
	entries3 := []*entry.Entry{
		{Key: []byte("apple"), Value: []byte("apple3")},
		{Key: []byte("banana"), Value: []byte("banana3")},
	}

	mergedData, err := Merge(entries1, entries2, entries3)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	expected := []*entry.Entry{
		{Key: []byte("apple"), Value: []byte("apple3")},
		{Key: []byte("banana"), Value: []byte("banana3")},
		{Key: []byte("carrot"), Value: []byte("carrot2")},
		{Key: []byte("mango"), Value: []byte("mango1")},
	}

	if len(mergedData) != len(expected) {
		t.Fatalf("Expected %d entries, got %d", len(expected), len(mergedData))
	}

	for i, e := range mergedData {
		if !bytes.Equal(e.Key, expected[i].Key) || !bytes.Equal(e.Value, expected[i].Value) {
			t.Errorf("Entry %d mismatch: expected %+v, got %+v", i, expected[i], e)
		}
	}
}
