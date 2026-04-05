package bloom

import "testing"

func TestBloomFilter_Add_Contains(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	bf.Add([]byte("test"))
	bf.Add([]byte("hello"))
	bf.Add([]byte("world"))

	if !bf.Contains([]byte("test")) {
		t.Errorf("Expected bloom filter to contain 'test'")
	}
	if !bf.Contains([]byte("hello")) {
		t.Errorf("Expected bloom filter to contain 'hello'")
	}
	if !bf.Contains([]byte("world")) {
		t.Errorf("Expected bloom filter to contain 'world'")
	}
	if bf.Contains([]byte("not_in_filter")) {
		t.Errorf("Expected bloom filter to NOT contain 'not_in_filter'")
	}
}
