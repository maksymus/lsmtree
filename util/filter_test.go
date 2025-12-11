package util

import (
	"testing"
)

func TestBloomFilter_Add_Contains(t *testing.T) {
	bloomFilter := NewBloomFilter(100, 0.01)

	bloomFilter.Add([]byte("test"))
	bloomFilter.Add([]byte("hello"))
	bloomFilter.Add([]byte("world"))

	if !bloomFilter.Contains([]byte("test")) {
		t.Errorf("Expected bloom filter to contain 'test'")
	}

	if !bloomFilter.Contains([]byte("hello")) {
		t.Errorf("Expected bloom filter to contain 'hello'")
	}

	if !bloomFilter.Contains([]byte("world")) {
		t.Errorf("Expected bloom filter to contain 'world'")
	}

	if bloomFilter.Contains([]byte("not_in_filter")) {
		t.Errorf("Expected bloom filter to NOT contain 'not_in_filter'")
	}
}
