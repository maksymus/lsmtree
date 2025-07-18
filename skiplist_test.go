package main

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestSkipList_Insert(t *testing.T) {
	type args struct {
		key   []byte
		value int
	}
	type testCase struct {
		name string
		sl   *SkipList[int]
		args args
	}
	tests := []testCase{
		{
			name: "Insert single element",
			sl:   NewSkipList[int](5, rand.New(rand.NewSource(0))),
			args: args{
				key:   []byte("key1"),
				value: 1,
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
	list := NewSkipList[int](5, rand.New(rand.NewSource(time.Now().Unix())))
	list.Insert([]byte("key1"), 1)
	list.Insert([]byte("key2"), 2)
	list.Insert([]byte("key5"), 5)
	list.Insert([]byte("key3"), 3)

	tests := []struct {
		key   []byte
		value int
		found bool
	}{
		{[]byte("key1"), 1, true},
		{[]byte("key2"), 2, true},
		{[]byte("key5"), 5, true},
		{[]byte("key3"), 3, true},
		{[]byte("key10"), 0, false}, // Non-existing key
	}

	for _, tt := range tests {
		value, found := list.Get(tt.key)
		if value != tt.value || found != tt.found {
			t.Errorf("Get(%s) = (%d, %v), want (%d, %v)", tt.key, value, found, tt.value, tt.found)
		}
	}
}

func TestSkipList_InsertRandom(t *testing.T) {
	list := NewSkipList[int](5, rand.New(rand.NewSource(time.Now().Unix())))
	numElements := 10000
	keys := make([][]byte, numElements)
	for i := 0; i < numElements; i++ {
		keys[i] = []byte(fmt.Sprintf("key%d", i))
		list.Insert(keys[i], i)
	}

	for i, key := range keys {
		value, found := list.Get(key)
		if !found || value != i {
			t.Errorf("Expected to find key %s with value %d, got (%d, %v)", key, i, value, found)
		}
	}
}

func TestSkipList_InsertDelete(t *testing.T) {
	list := NewSkipList[int](5, rand.New(rand.NewSource(time.Now().Unix())))
	list.Insert([]byte("key1"), 1)
	list.Insert([]byte("key2"), 2)

	// Simulate deletion by not implementing it, but we can check if the keys still exist
	value, found := list.Get([]byte("key1"))
	if !found || value != 1 {
		t.Errorf("Expected to find key 'key1' with value 1, got (%d, %v)", value, found)
	}

	value, found = list.Get([]byte("key2"))
	if !found || value != 2 {
		t.Errorf("Expected to find key 'key2' with value 2, got (%d, %v)", value, found)
	}

	deleted := list.Delete([]byte("key1"))
	if !deleted {
		t.Errorf("Expected to delete key 'key1', but it was not deleted")
	}

	value, found = list.Get([]byte("key1"))
	if found {
		t.Errorf("Expected key 'key1' to be deleted, but found with value %d", value)
	}
}

func TestSkipList_Update(t *testing.T) {
	list := NewSkipList[int](5, rand.New(rand.NewSource(time.Now().Unix())))
	list.Insert([]byte("key1"), 1)
	list.Insert([]byte("key2"), 2)

	// Update key1
	found := list.Update([]byte("key1"), 10)
	if !found {
		t.Errorf("Expected to update key 'key1', but it was not found")
	}

	notFound := list.Update([]byte("key3"), 3)
	if notFound {
		t.Errorf("Expected to not find key 'key3' for update, but it was found")
	}

	value, found := list.Get([]byte("key1"))
	if !found || value != 10 {
		t.Errorf("Expected to find key 'key1' with updated value 10, got (%d, %v)", value, found)
	}

	value, found = list.Get([]byte("key2"))
	if !found || value != 2 {
		t.Errorf("Expected to find key 'key2' with value 2, got (%d, %v)", value, found)
	}
}

func TestSkipList_All(t *testing.T) {
	list := NewSkipList[int](5, rand.New(rand.NewSource(time.Now().Unix())))
	list.Insert([]byte("key1"), 1)
	list.Insert([]byte("key2"), 2)
	list.Insert([]byte("key3"), 3)

	// Check all keys
	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	for _, key := range keys {
		value, found := list.Get(key)
		if !found {
			t.Errorf("Expected to find key %s, but it was not found", key)
		} else {
			fmt.Printf("Found key %s with value %d\n", key, value)
		}
	}
}
