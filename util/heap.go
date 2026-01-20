package util

import "container/heap"

// Heap is a generic binary heap wrapper around the standard library's
// container/heap implementation. It accepts a comparator function `less`
// that defines the heap ordering (return true when a < b). Use NewHeap to
// construct a Heap specialized for the desired element type and ordering.
type Heap[T any] struct {
	inner *heapImpl[T]
}

// NewHeap creates and returns a new Heap using the provided less function
// to compare two elements. The returned Heap is ready for Push/Pop use.
func NewHeap[T any](less func(a, b T) bool) *Heap[T] {
	inner := &heapImpl[T]{
		items: make([]T, 0),
		less:  less,
	}

	heap.Init(inner)

	return &Heap[T]{
		inner: inner,
	}
}

// NewHeapWithCapacity creates a new Heap with an initial backing array
// capacity. Use this when you know the approximate number of elements to
// push to avoid repeated allocations during growth.
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

// Push pushes item onto the heap. This is a thin wrapper over
// container/heap.Push and is safe for any element of the Heap's type.
func (h *Heap[T]) Push(item T) {
	heap.Push(h.inner, item)
}

// Pop removes and returns the top element from the heap. The boolean
// return value is false if the heap was empty.
func (h *Heap[T]) Pop() (T, bool) {
	if h.inner.Len() == 0 {
		var zero T
		return zero, false
	}

	item := heap.Pop(h.inner).(T)
	return item, true
}

// Len returns the number of elements currently stored in the heap.
func (h *Heap[T]) Len() int {
	return h.inner.Len()
}

// Peek returns the top element without removing it. The boolean return
// value is false if the heap is empty.
func (h *Heap[T]) Peek() (T, bool) {
	if h.inner.Len() == 0 {
		var zero T
		return zero, false
	}

	return h.inner.items[0], true
}

// heapImpl is the concrete implementation of the heap for type T. It
// implements container/heap.Interface. This type is internal to the
// util package and should not be used directly by callers.
type heapImpl[T any] struct {
	items []T
	less  func(a, b T) bool
}

// Len implements heap.Interface.Len.
func (ih *heapImpl[T]) Len() int {
	return len(ih.items)
}

// Less implements heap.Interface.Less using the provided comparator.
func (ih *heapImpl[T]) Less(i, j int) bool {
	return ih.less(ih.items[i], ih.items[j])
}

// Swap implements heap.Interface.Swap.
func (ih *heapImpl[T]) Swap(i, j int) {
	ih.items[i], ih.items[j] = ih.items[j], ih.items[i]
}

// Push implements heap.Interface.Push. The parameter is of type any to
// satisfy the interface; it will be asserted back to T.
func (ih *heapImpl[T]) Push(x any) {
	ih.items = append(ih.items, x.(T))
}

// Pop implements heap.Interface.Pop and returns the last element.
func (ih *heapImpl[T]) Pop() any {
	n := len(ih.items)
	item := ih.items[n-1]
	ih.items = ih.items[0 : n-1]
	return item
}
