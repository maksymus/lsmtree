package util

import "testing"

func TestHeap_PushPop(t *testing.T) {
	maxHeap := NewHeap[int](func(a, b int) bool {
		return a > b
	})

	numbers := []int{5, 3, 8, 1, 2, 7}
	for _, num := range numbers {
		maxHeap.Push(num)
	}

	expectedOrder := []int{8, 7, 5, 3, 2, 1}
	for _, expected := range expectedOrder {
		value, _ := maxHeap.Pop()
		if value != expected {
			t.Errorf("Expected %d, got %d", expected, value)
		}
	}
}

func TestHeap_Peek(t *testing.T) {
	minHeap := NewHeap[int](func(a, b int) bool {
		return a < b
	})

	minHeap.Push(5)
	minHeap.Push(3)
	minHeap.Push(8)

	val, ok := minHeap.Peek()
	if !ok {
		t.Fatal("Peek() returned false on non-empty heap")
	}
	if val != 3 {
		t.Errorf("Peek() = %d, want 3", val)
	}

	// Peek should not remove the element
	if minHeap.Len() != 3 {
		t.Errorf("Len() after Peek() = %d, want 3", minHeap.Len())
	}

	// Peek again should return the same value
	val2, _ := minHeap.Peek()
	if val2 != 3 {
		t.Errorf("second Peek() = %d, want 3", val2)
	}
}

func TestHeap_Peek_Empty(t *testing.T) {
	h := NewHeap[int](func(a, b int) bool { return a < b })

	val, ok := h.Peek()
	if ok {
		t.Fatal("Peek() on empty heap returned true")
	}
	if val != 0 {
		t.Errorf("Peek() on empty heap = %d, want 0 (zero value)", val)
	}
}

func TestHeap_Len(t *testing.T) {
	h := NewHeap[int](func(a, b int) bool { return a < b })

	if h.Len() != 0 {
		t.Errorf("Len() on new heap = %d, want 0", h.Len())
	}

	h.Push(10)
	if h.Len() != 1 {
		t.Errorf("Len() after 1 Push = %d, want 1", h.Len())
	}

	h.Push(20)
	h.Push(30)
	if h.Len() != 3 {
		t.Errorf("Len() after 3 Pushes = %d, want 3", h.Len())
	}

	h.Pop()
	if h.Len() != 2 {
		t.Errorf("Len() after Pop = %d, want 2", h.Len())
	}
}

func TestHeap_Pop_Empty(t *testing.T) {
	h := NewHeap[int](func(a, b int) bool { return a < b })

	val, ok := h.Pop()
	if ok {
		t.Fatal("Pop() on empty heap returned true")
	}
	if val != 0 {
		t.Errorf("Pop() on empty heap = %d, want 0 (zero value)", val)
	}
}

func TestHeap_Pop_Empty_String(t *testing.T) {
	h := NewHeap[string](func(a, b string) bool { return a < b })

	val, ok := h.Pop()
	if ok {
		t.Fatal("Pop() on empty string heap returned true")
	}
	if val != "" {
		t.Errorf("Pop() on empty string heap = %q, want empty string", val)
	}
}
