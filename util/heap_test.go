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
