package wal

import (
	"bytes"
	"strings"
	"testing"

	"github.com/maksymus/lmstree/entry"
	"github.com/maksymus/lmstree/internal/pool"
)

type InMemoryWalFile struct {
	buffer bytes.Buffer
}

func (i *InMemoryWalFile) Read(p []byte) (n int, err error)        { return i.buffer.Read(p) }
func (i *InMemoryWalFile) Write(p []byte) (n int, err error)       { return i.buffer.Write(p) }
func (i *InMemoryWalFile) Seek(offset int64, whence int) (int64, error) { return 0, nil }
func (i *InMemoryWalFile) Close() error                             { return nil }

func NewInMemoryWalFile() WalFile {
	return &InMemoryWalFile{buffer: bytes.Buffer{}}
}

func TestWAL_Write(t *testing.T) {
	tests := []struct {
		name    string
		entries []*entry.Entry
		wantErr bool
	}{
		{
			name:    "write single entry",
			entries: []*entry.Entry{{Key: []byte("key1"), Value: []byte("value1")}},
			wantErr: false,
		},
		{
			name: "write multiple entries",
			entries: []*entry.Entry{
				{Key: []byte("key1"), Value: []byte("value1")},
				{Key: []byte("key2"), Value: []byte("value2")},
				{Key: []byte("key3"), Value: []byte("value3")},
			},
			wantErr: false,
		},
		{
			name:    "write no entries",
			entries: []*entry.Entry{},
			wantErr: false,
		},
		{
			name:    "write nil entry",
			entries: []*entry.Entry{nil},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &WAL{file: NewInMemoryWalFile(), pool: pool.NewBytesBufferPool()}
			if err := w.Write(tt.entries...); (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWAL_Read(t *testing.T) {
	w := &WAL{file: NewInMemoryWalFile(), pool: pool.NewBytesBufferPool()}

	entries := []*entry.Entry{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2"), Tombstone: true},
		{Key: []byte("key3"), Value: []byte("value3")},
	}

	if err := w.Write(entries...); err != nil {
		t.Fatalf("Write() error: %v", err)
	}

	got, err := w.Read()
	if err != nil {
		t.Fatalf("Read() error: %v", err)
	}
	if len(got) != len(entries) {
		t.Fatalf("Read() returned %d entries, want %d", len(got), len(entries))
	}

	for i, e := range entries {
		if !bytes.Equal(got[i].Key, e.Key) {
			t.Errorf("entry[%d].Key = %s, want %s", i, got[i].Key, e.Key)
		}
		if !bytes.Equal(got[i].Value, e.Value) {
			t.Errorf("entry[%d].Value = %s, want %s", i, got[i].Value, e.Value)
		}
		if got[i].Tombstone != e.Tombstone {
			t.Errorf("entry[%d].Tombstone = %v, want %v", i, got[i].Tombstone, e.Tombstone)
		}
	}
}

func TestWAL_Close(t *testing.T) {
	w := &WAL{file: NewInMemoryWalFile(), pool: pool.NewBytesBufferPool()}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	if err := w.Close(); err == nil {
		t.Fatal("expected error on double Close()")
	}
}

func TestWAL_Close_WriteAfterClose(t *testing.T) {
	w := &WAL{file: NewInMemoryWalFile(), pool: pool.NewBytesBufferPool()}
	w.Close()
	err := w.Write(&entry.Entry{Key: []byte("k"), Value: []byte("v")})
	if err == nil {
		t.Fatal("expected error writing to closed WAL")
	}
}

func TestWAL_CompareVersion(t *testing.T) {
	tests := []struct {
		name     string
		version  string
		other    string
		expected int
	}{
		{"equal versions", "20250101120000-123456", "20250101120000-123456", 0},
		{"this version is less (timestamp)", "20250101110000-123456", "20250101120000-123456", -1},
		{"this version is greater (timestamp)", "20250101130000-123456", "20250101120000-123456", 1},
		{"this version is less (nanoseconds)", "20250101120000-100000", "20250101120000-200000", -1},
		{"this version is greater (nanoseconds)", "20250101120000-300000", "20250101120000-200000", 1},
		{"malformed version returns 0", "bad", "20250101120000-123456", 0},
		{"malformed other returns 0", "20250101120000-123456", "bad", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &WAL{version: tt.version}
			got := w.CompareVersion(tt.other)
			if got != tt.expected {
				t.Errorf("CompareVersion(%q) = %d, want %d", tt.other, got, tt.expected)
			}
		})
	}
}

func TestVersionFromFileName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{"valid filename", "wal-20250101120000-123456789.log", "20250101120000-123456789", false},
		{"invalid prefix", "notwal-20250101120000-123456789.log", "", true},
		{"invalid extension", "wal-20250101120000-123456789.txt", "", true},
		{"missing nanoseconds", "wal-20250101120000.log", "", true},
		{"empty string", "", "", true},
		{"non-numeric parts", "wal-abc-def.log", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := VersionFromFileName(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("VersionFromFileName(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("VersionFromFileName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestNoopWAL_Write(t *testing.T) {
	n := &NoopWAL{}
	if err := n.Write(&entry.Entry{Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatalf("NoopWAL.Write() returned error: %v", err)
	}
}

func TestNoopWAL_CompareVersion(t *testing.T) {
	n := &NoopWAL{}
	if got := n.CompareVersion("20250101120000-123456"); got != 1 {
		t.Fatalf("NoopWAL.CompareVersion() = %d, want 1", got)
	}
}

func TestWAL_Delete_InMemory(t *testing.T) {
	w := &WAL{
		file: NewInMemoryWalFile(),
		pool: pool.NewBytesBufferPool(),
		path: "/nonexistent/path/for/test.log",
	}

	err := w.Delete()
	if err == nil {
		t.Fatal("expected error from os.Remove on nonexistent path")
	}
	if w.file != nil {
		t.Fatal("expected file to be nil after Delete()")
	}
	if !strings.Contains(err.Error(), "test.log") {
		t.Errorf("expected error to mention file path, got: %v", err)
	}
}
