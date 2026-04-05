package heap

import (
	"container/heap"
	"math/rand"
	"testing"
)

func BenchmarkHeap_BuildAndDrain(b *testing.B) {
	const N = 1024
	vals := make([]int, N)
	rnd := rand.New(rand.NewSource(42))
	for i := 0; i < N; i++ {
		vals[i] = rnd.Int()
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h := NewHeapWithCapacity[int](N, func(a, b int) bool { return a < b })
		for j := 0; j < N; j++ {
			h.Push(vals[j])
		}
		for h.Len() > 0 {
			h.Pop()
		}
	}
}

func BenchmarkHeap_BuildByHeapify(b *testing.B) {
	const N = 1024
	vals := make([]int, N)
	rnd := rand.New(rand.NewSource(42))
	for i := 0; i < N; i++ {
		vals[i] = rnd.Int()
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h := NewHeapWithCapacity[int](N, func(a, b int) bool { return a < b })
		h.inner.items = append(h.inner.items, vals...)
		heap.Init(h.inner)
		for h.Len() > 0 {
			h.Pop()
		}
	}
}

func BenchmarkHeap_PushPop_Amortized(b *testing.B) {
	const K = 256
	vals := make([]int, K)
	rnd := rand.New(rand.NewSource(99))
	for i := 0; i < K; i++ {
		vals[i] = rnd.Int()
	}

	h := NewHeap(func(a, b int) bool { return a < b })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Push(vals[i%K])
		h.Pop()
	}
}

func BenchmarkHeap_Peek(b *testing.B) {
	const N = 1024
	vals := make([]int, N)
	rnd := rand.New(rand.NewSource(7))
	for i := 0; i < N; i++ {
		vals[i] = rnd.Int()
	}

	h := NewHeap(func(a, b int) bool { return a < b })
	for i := 0; i < N; i++ {
		h.Push(vals[i])
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = h.Peek()
	}
}
