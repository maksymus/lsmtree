package heap

import "container/heap"

// Heap is a generic binary heap wrapper around the standard library's
// container/heap implementation.
type Heap[T any] struct {
	inner *heapImpl[T]
}

// NewHeap creates and returns a new Heap using the provided less function.
func NewHeap[T any](less func(a, b T) bool) *Heap[T] {
	inner := &heapImpl[T]{
		items: make([]T, 0),
		less:  less,
	}
	heap.Init(inner)
	return &Heap[T]{inner: inner}
}

// NewHeapWithCapacity creates a new Heap with an initial backing array capacity.
func NewHeapWithCapacity[T any](cap int, less func(a, b T) bool) *Heap[T] {
	if cap < 0 {
		cap = 0
	}
	inner := &heapImpl[T]{
		items: make([]T, 0, cap),
		less:  less,
	}
	heap.Init(inner)
	return &Heap[T]{inner: inner}
}

// Push pushes item onto the heap.
func (h *Heap[T]) Push(item T) {
	heap.Push(h.inner, item)
}

// Pop removes and returns the top element. Returns false if empty.
func (h *Heap[T]) Pop() (T, bool) {
	if h.inner.Len() == 0 {
		var zero T
		return zero, false
	}
	item := heap.Pop(h.inner).(T)
	return item, true
}

// Len returns the number of elements in the heap.
func (h *Heap[T]) Len() int {
	return h.inner.Len()
}

// Peek returns the top element without removing it. Returns false if empty.
func (h *Heap[T]) Peek() (T, bool) {
	if h.inner.Len() == 0 {
		var zero T
		return zero, false
	}
	return h.inner.items[0], true
}

type heapImpl[T any] struct {
	items []T
	less  func(a, b T) bool
}

func (ih *heapImpl[T]) Len() int           { return len(ih.items) }
func (ih *heapImpl[T]) Less(i, j int) bool { return ih.less(ih.items[i], ih.items[j]) }
func (ih *heapImpl[T]) Swap(i, j int)      { ih.items[i], ih.items[j] = ih.items[j], ih.items[i] }

func (ih *heapImpl[T]) Push(x any) {
	ih.items = append(ih.items, x.(T))
}

func (ih *heapImpl[T]) Pop() any {
	n := len(ih.items)
	item := ih.items[n-1]
	ih.items = ih.items[0 : n-1]
	return item
}
