package skiplist

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestSkipList_Insert(t *testing.T) {
	sl := NewSkipList(5, rand.New(rand.NewSource(0)))
	sl.Insert([]byte("key1"), []byte{1})
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
		{[]byte("key10"), []byte{}, false},
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

	value, found := list.Get([]byte("key1"))
	if bytes.Compare(value, []byte{1}) != 0 || !found {
		t.Errorf("Expected to find Key 'key1' with Value 1, got (%d, %v)", value, found)
	}

	deleted := list.Delete([]byte("key1"))
	if !deleted {
		t.Errorf("Expected to delete Key 'key1', but it was not deleted")
	}

	_, found = list.Get([]byte("key1"))
	if found {
		t.Errorf("Expected Key 'key1' to be deleted")
	}
}

func TestSkipList_Update(t *testing.T) {
	list := NewSkipList(5, rand.New(rand.NewSource(time.Now().Unix())))
	list.Insert([]byte("key1"), []byte{1})
	list.Insert([]byte("key2"), []byte{2})

	if !list.Update([]byte("key1"), []byte{10}) {
		t.Errorf("Expected to update Key 'key1'")
	}
	if list.Update([]byte("key3"), []byte{3}) {
		t.Errorf("Expected to not find Key 'key3' for update")
	}

	value, found := list.Get([]byte("key1"))
	if bytes.Compare(value, []byte{10}) != 0 || !found {
		t.Errorf("Expected to find Key 'key1' with updated Value 10, got (%d, %v)", value, found)
	}
}

func TestSkipList_All_SortedOrder(t *testing.T) {
	list := NewSkipList(5, rand.New(rand.NewSource(0)))
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
	if len(list.All()) != 0 {
		t.Fatal("All() on empty list returned non-empty result")
	}
}

func TestSkipList_Reset(t *testing.T) {
	list := NewSkipList(5, rand.New(rand.NewSource(0)))
	list.Insert([]byte("key1"), []byte{1})
	list.Insert([]byte("key2"), []byte{2})
	list.Reset()

	if len(list.All()) != 0 {
		t.Fatal("All() after Reset returned non-empty result")
	}
	_, found := list.Get([]byte("key1"))
	if found {
		t.Error("expected key1 to not be found after Reset")
	}
	list.Insert([]byte("newkey"), []byte{42})
	val, found := list.Get([]byte("newkey"))
	if !found || !bytes.Equal(val, []byte{42}) {
		t.Errorf("expected to find newkey after Reset+Insert")
	}
}

func TestSkipList_LowerBound(t *testing.T) {
	list := NewSkipList(5, rand.New(rand.NewSource(0)))
	list.Insert([]byte("b"), []byte("B"))
	list.Insert([]byte("d"), []byte("D"))
	list.Insert([]byte("f"), []byte("F"))

	tests := []struct {
		key       []byte
		wantValue []byte
		wantFound bool
	}{
		{[]byte("d"), []byte("D"), true},
		{[]byte("c"), []byte("D"), true},
		{[]byte("a"), []byte("B"), true},
		{[]byte("g"), nil, false},
	}

	for _, tt := range tests {
		val, found := list.LowerBound(tt.key)
		if found != tt.wantFound {
			t.Errorf("LowerBound(%s) found = %v, want %v", tt.key, found, tt.wantFound)
		}
		if !bytes.Equal(val, tt.wantValue) {
			t.Errorf("LowerBound(%s) = %s, want %s", tt.key, val, tt.wantValue)
		}
	}
}

func TestSkipList_LowerBound_Empty(t *testing.T) {
	list := NewSkipList(5, rand.New(rand.NewSource(0)))
	_, found := list.LowerBound([]byte("a"))
	if found {
		t.Error("expected LowerBound on empty list to return false")
	}
}
