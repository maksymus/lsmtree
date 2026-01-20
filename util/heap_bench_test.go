package util

import (
	"container/heap"
	"math/rand"
	"testing"
)

// BenchmarkHeap_BuildAndDrain measures the cost to build a heap by pushing
// N items and then draining (pop all). This exercises the allocation and
// comparison behavior for bulk operations.
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

// BenchmarkHeap_BuildByHeapify measures building a heap by pre-filling the
// backing slice and calling heap.Init, which performs an O(n) heap build.
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
		// directly set the internal slice to avoid per-push work
		h.inner.items = append(h.inner.items, vals...)
		heap.Init(h.inner)
		for h.Len() > 0 {
			h.Pop()
		}
	}
}

// BenchmarkHeap_PushPop_Amortized keeps the heap size small by pushing and
// immediately popping, measuring per-op cost without repeated growth.
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

// BenchmarkHeap_Peek measures the cost of repeated Peek calls on a
// populated heap. Peek should be cheap and allocation-free.
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
