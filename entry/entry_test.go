package entry

import "testing"

func TestEntry_Size(t *testing.T) {
	tests := []struct {
		name  string
		entry Entry
		want  int
	}{
		{"normal entry", Entry{Key: []byte("hello"), Value: []byte("world")}, 11},
		{"empty key and value", Entry{Key: []byte{}, Value: []byte{}}, 1},
		{"nil key and value", Entry{}, 1},
		{"tombstone entry", Entry{Key: []byte("k"), Value: []byte("v"), Tombstone: true}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.entry.Size(); got != tt.want {
				t.Errorf("Size() = %d, want %d", got, tt.want)
			}
		})
	}
}
