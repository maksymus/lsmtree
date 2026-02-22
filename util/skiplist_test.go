package util

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestSkipList_Insert(t *testing.T) {
	type args struct {
		key   []byte
		value []byte
	}
	type testCase struct {
		name string
		sl   *SkipList
		args args
	}
	tests := []testCase{
		{
			name: "Insert single element",
			sl:   NewSkipList(5, rand.New(rand.NewSource(0))),
			args: args{
				key:   []byte("key1"),
				value: []byte{1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.sl.Insert(tt.args.key, tt.args.value)
		})
	}
}

func TestSkipList_InsertAndGet(t *testing.T) {
	list := NewSkipList(5, rand.New(rand.NewSource(time.Now().Unix())))
	list.Insert([]byte("key1"), []byte{1})
	list.Insert([]byte("key2"), []byte{2})
	list.Insert([]byte("key5"), []byte{5})
	list.Insert([]byte("key3"), []byte{3})

	tests := []struct {
		key   []byte
		value []byte
		found bool
	}{
		{[]byte("key1"), []byte{1}, true},
		{[]byte("key2"), []byte{2}, true},
		{[]byte("key5"), []byte{5}, true},
		{[]byte("key3"), []byte{3}, true},
		{[]byte("key10"), []byte{}, false}, // Non-existing Key
	}

	for _, tt := range tests {
		value, found := list.Get(tt.key)
		if bytes.Compare(value, tt.value) != 0 || found != tt.found {
			t.Errorf("Get(%s) = (%d, %v), want (%d, %v)", tt.key, value, found, tt.value, tt.found)
		}
	}
}

func TestSkipList_InsertRandom(t *testing.T) {
	list := NewSkipList(5, rand.New(rand.NewSource(time.Now().Unix())))
	numElements := 10000
	keys := make([][]byte, numElements)
	for i := 0; i < numElements; i++ {
		keys[i] = []byte(fmt.Sprintf("Key%d", i))
		list.Insert(keys[i], []byte{byte(i)})
	}

	for i, key := range keys {
		value, found := list.Get(key)
		if bytes.Compare(value, []byte{byte(i)}) != 0 || !found {
			t.Errorf("Expected to find Key %s with Value %d, got (%d, %v)", key, i, value, found)
		}
	}
}

func TestSkipList_InsertDelete(t *testing.T) {
	list := NewSkipList(5, rand.New(rand.NewSource(time.Now().Unix())))
	list.Insert([]byte("key1"), []byte{1})
	list.Insert([]byte("key2"), []byte{2})

	// Simulate deletion by not implementing it, but we can check if the keys still exist
	value, found := list.Get([]byte("key1"))
	if bytes.Compare(value, []byte{1}) != 0 || !found {
		t.Errorf("Expected to find Key 'key1' with Value 1, got (%d, %v)", value, found)
	}

	value, found = list.Get([]byte("key2"))
	if bytes.Compare(value, []byte{2}) != 0 || !found {
		t.Errorf("Expected to find Key 'key2' with Value 2, got (%d, %v)", value, found)
	}

	deleted := list.Delete([]byte("key1"))
	if !deleted {
		t.Errorf("Expected to delete Key 'key1', but it was not deleted")
	}

	value, found = list.Get([]byte("key1"))
	if found {
		t.Errorf("Expected Key 'key1' to be deleted, but found with Value %d", value)
	}
}

func TestSkipList_Update(t *testing.T) {
	list := NewSkipList(5, rand.New(rand.NewSource(time.Now().Unix())))
	list.Insert([]byte("key1"), []byte{1})
	list.Insert([]byte("key2"), []byte{2})

	// Update key1
	found := list.Update([]byte("key1"), []byte{10})
	if !found {
		t.Errorf("Expected to update Key 'key1', but it was not found")
	}

	notFound := list.Update([]byte("key3"), []byte{3})
	if notFound {
		t.Errorf("Expected to not find Key 'key3' for update, but it was found")
	}

	value, found := list.Get([]byte("key1"))
	if bytes.Compare(value, []byte{10}) != 0 || !found {
		t.Errorf("Expected to find Key 'key1' with updated Value 10, got (%d, %v)", value, found)
	}

	value, found = list.Get([]byte("key2"))
	if bytes.Compare(value, []byte{2}) != 0 || !found {
		t.Errorf("Expected to find Key 'key2' with Value 2, got (%d, %v)", value, found)
	}
}

func TestSkipList_All(t *testing.T) {
	list := NewSkipList(5, rand.New(rand.NewSource(time.Now().Unix())))
	list.Insert([]byte("key1"), []byte{1})
	list.Insert([]byte("key2"), []byte{2})
	list.Insert([]byte("key3"), []byte{3})

	// Check all keys
	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	for _, key := range keys {
		value, found := list.Get(key)
		if !found {
			t.Errorf("Expected to find Key %s, but it was not found", key)
		} else {
			fmt.Printf("Found Key %s with Value %d\n", key, value)
		}
	}
}

func TestSkipList_All_SortedOrder(t *testing.T) {
	list := NewSkipList(5, rand.New(rand.NewSource(0)))
	// Insert out of order
	list.Insert([]byte("cherry"), []byte("c"))
	list.Insert([]byte("apple"), []byte("a"))
	list.Insert([]byte("banana"), []byte("b"))

	values := list.All()
	if len(values) != 3 {
		t.Fatalf("All() returned %d values, want 3", len(values))
	}

	expected := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	for i, val := range values {
		if !bytes.Equal(val, expected[i]) {
			t.Errorf("All()[%d] = %s, want %s", i, val, expected[i])
		}
	}
}

func TestSkipList_All_Empty(t *testing.T) {
	list := NewSkipList(5, rand.New(rand.NewSource(0)))
	values := list.All()
	if len(values) != 0 {
		t.Fatalf("All() on empty list returned %d values, want 0", len(values))
	}
}

func TestSkipList_Reset(t *testing.T) {
	list := NewSkipList(5, rand.New(rand.NewSource(0)))
	list.Insert([]byte("key1"), []byte{1})
	list.Insert([]byte("key2"), []byte{2})
	list.Insert([]byte("key3"), []byte{3})

	list.Reset()

	// All values should be gone
	values := list.All()
	if len(values) != 0 {
		t.Fatalf("All() after Reset returned %d values, want 0", len(values))
	}

	// Get should not find anything
	_, found := list.Get([]byte("key1"))
	if found {
		t.Error("expected key1 to not be found after Reset")
	}

	// Should be able to insert again after Reset
	list.Insert([]byte("newkey"), []byte{42})
	val, found := list.Get([]byte("newkey"))
	if !found || !bytes.Equal(val, []byte{42}) {
		t.Errorf("expected to find newkey after Reset+Insert, got (%v, %v)", val, found)
	}
}

func TestSkipList_LowerBound(t *testing.T) {
	list := NewSkipList(5, rand.New(rand.NewSource(0)))
	list.Insert([]byte("b"), []byte("B"))
	list.Insert([]byte("d"), []byte("D"))
	list.Insert([]byte("f"), []byte("F"))

	tests := []struct {
		name      string
		key       []byte
		wantValue []byte
		wantFound bool
	}{
		{
			name:      "exact match",
			key:       []byte("d"),
			wantValue: []byte("D"),
			wantFound: true,
		},
		{
			name:      "between keys",
			key:       []byte("c"),
			wantValue: []byte("D"),
			wantFound: true,
		},
		{
			name:      "before first key",
			key:       []byte("a"),
			wantValue: []byte("B"),
			wantFound: true,
		},
		{
			name:      "after last key",
			key:       []byte("g"),
			wantValue: nil,
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, found := list.LowerBound(tt.key)
			if found != tt.wantFound {
				t.Errorf("LowerBound(%s) found = %v, want %v", tt.key, found, tt.wantFound)
			}
			if !bytes.Equal(val, tt.wantValue) {
				t.Errorf("LowerBound(%s) = %s, want %s", tt.key, val, tt.wantValue)
			}
		})
	}
}

func TestSkipList_LowerBound_Empty(t *testing.T) {
	list := NewSkipList(5, rand.New(rand.NewSource(0)))
	_, found := list.LowerBound([]byte("a"))
	if found {
		t.Error("expected LowerBound on empty list to return false")
	}
}
