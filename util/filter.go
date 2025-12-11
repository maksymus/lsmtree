package util

import (
	"hash"
	"math"

	"github.com/spaolacci/murmur3"
)

// BloomFilter is a probabilistic data structure that is used to test whether an element is a member of a set.
// It is space-efficient and allows for false positives, meaning it can indicate that an element is in the set when it is not.
// The BloomFilter uses a bit array and multiple hash functions to determine membership.
// It is particularly useful in scenarios where the set is large and memory efficiency is crucial, such as in databases or network applications.
// A BloomFilter can be used to quickly check if an element is possibly in the set, without storing the actual elements.
// The BloomFilter struct contains a bit array and a hash function to manage the membership of elements.
// The bit array is represented as a slice of booleans, where each index corresponds to a bit.
// The hash function is used to compute the indices in the bit array that should be set for each element added to the BloomFilter.
type BloomFilter struct {
	bitsets []bool
	hashes  []hash.Hash32
}

// NewBloomFilter creates a new BloomFilter with the specified number of elements and desired false positive probability.
// The `num` parameter specifies the expected number of elements to be added to the BloomFilter.
// The `prob` parameter specifies the desired probability of false positives.
func NewBloomFilter(num int, prob float64) *BloomFilter {

	// Calculate the size of the bit array based on the number of elements and the desired false positive probability.
	m := int(-float64(num) * math.Log(prob) / (math.Log(2) * math.Log(2)))

	// Calculate the number of hash functions to use based on the size of the bit array and the number of elements.
	k := int(math.Ceil(float64(m) / float64(num) * math.Log(2)))

	hashes := make([]hash.Hash32, k)
	for i := 0; i < k; i++ {
		hashes[i] = murmur3.New32WithSeed(uint32(i))
	}

	return &BloomFilter{
		bitsets: make([]bool, m),
		hashes:  hashes,
	}
}

// Add adds an element to the BloomFilter.
func (bf *BloomFilter) Add(data []byte) {
	for _, h := range bf.hashes {
		h.Reset()
		_, _ = h.Write(data)
		index := h.Sum32() % uint32(len(bf.bitsets))
		bf.bitsets[index] = true
	}
}

// Contains checks if an element is possibly in the BloomFilter.
func (bf *BloomFilter) Contains(data []byte) bool {
	for _, h := range bf.hashes {
		h.Reset()
		_, _ = h.Write(data)
		index := h.Sum32() % uint32(len(bf.bitsets))
		if !bf.bitsets[index] {
			return false // If any bit is not set, the element is definitely not in the filter.
		}
	}
	return true // All bits are set, so the element is possibly in the filter.
}
