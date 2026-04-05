package bloom

import (
	"encoding/binary"
	"fmt"
	"hash"
	"math"

	"github.com/spaolacci/murmur3"
)

// BloomFilter is a probabilistic data structure that is used to test whether an element is a member of a set.
type BloomFilter struct {
	bitsets []bool
	hashes  []hash.Hash32
}

// NewBloomFilter creates a new BloomFilter with the specified number of elements and desired false positive probability.
func NewBloomFilter(num int, prob float64) *BloomFilter {
	m := int(-float64(num) * math.Log(prob) / (math.Log(2) * math.Log(2)))
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
			return false
		}
	}
	return true
}

// Encode serializes the BloomFilter to bytes.
// Format: k (4 bytes) | m (4 bytes) | packed bit array (ceil(m/8) bytes).
func (bf *BloomFilter) Encode() []byte {
	k := uint32(len(bf.hashes))
	m := uint32(len(bf.bitsets))
	numBytes := (m + 7) / 8
	buf := make([]byte, 8+numBytes)
	binary.BigEndian.PutUint32(buf[0:], k)
	binary.BigEndian.PutUint32(buf[4:], m)
	for i, set := range bf.bitsets {
		if set {
			buf[8+uint32(i)/8] |= 1 << (uint(i) % 8)
		}
	}
	return buf
}

// Decode reconstructs a BloomFilter from bytes produced by Encode.
func Decode(data []byte) (*BloomFilter, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("bloom filter data too short: %d bytes", len(data))
	}
	k := binary.BigEndian.Uint32(data[0:])
	m := binary.BigEndian.Uint32(data[4:])
	numBytes := (m + 7) / 8
	if uint32(len(data)) < 8+numBytes {
		return nil, fmt.Errorf("bloom filter data truncated: need %d, got %d", 8+numBytes, len(data))
	}
	bitsets := make([]bool, m)
	for i := range bitsets {
		bitsets[i] = data[8+uint32(i)/8]>>(uint(i)%8)&1 == 1
	}
	hashes := make([]hash.Hash32, k)
	for i := range hashes {
		hashes[i] = murmur3.New32WithSeed(uint32(i))
	}
	return &BloomFilter{bitsets: bitsets, hashes: hashes}, nil
}
