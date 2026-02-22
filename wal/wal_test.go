package wal

import (
	"bytes"
	"strings"
	"testing"

	"github.com/maksymus/lmstree/util"
)

type InMemoryWalFile struct {
	buffer bytes.Buffer
}

func (i *InMemoryWalFile) Read(p []byte) (n int, err error) {
	return i.buffer.Read(p)
}

func (i *InMemoryWalFile) Write(p []byte) (n int, err error) {
	return i.buffer.Write(p)
}

func (i *InMemoryWalFile) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (i *InMemoryWalFile) Close() error {
	return nil
}

func NewInMemoryWalFile() WalFile {
	return &InMemoryWalFile{
		buffer: bytes.Buffer{},
	}
}

func TestWAL_Write(t *testing.T) {
	type fields struct {
		file WalFile
	}
	type args struct {
		entries []*util.Entry
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "write single entry",
			fields: fields{
				file: NewInMemoryWalFile(),
			},
			args: args{
				entries: []*util.Entry{
					{Key: []byte("key1"), Value: []byte("value1")},
				},
			},
			wantErr: false,
		},
		{
			name: "write multiple entries",
			fields: fields{
				file: NewInMemoryWalFile(),
			},
			args: args{
				entries: []*util.Entry{
					{Key: []byte("key1"), Value: []byte("value1")},
					{Key: []byte("key2"), Value: []byte("value2")},
					{Key: []byte("key3"), Value: []byte("value3")},
				},
			},
			wantErr: false,
		},
		{
			name: "write no entries",
			fields: fields{
				file: NewInMemoryWalFile(),
			},
			args: args{
				entries: []*util.Entry{},
			},
			wantErr: false,
		},
		{
			name: "write nil entry",
			fields: fields{
				file: NewInMemoryWalFile(),
			},
			args: args{
				entries: []*util.Entry{nil},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &WAL{
				file: tt.fields.file,
				pool: util.NewBytesBufferPool(),
			}
			if err := w.Write(tt.args.entries...); (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}

			walFile := w.file.(*InMemoryWalFile)
			t.Logf("WAL buffer length: %d", walFile.buffer.Len())
		})
	}
}

func TestWAL_Read(t *testing.T) {
	w := &WAL{
		file: NewInMemoryWalFile(),
		pool: util.NewBytesBufferPool(),
	}

	entries := []*util.Entry{
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

	for i, entry := range entries {
		if !bytes.Equal(got[i].Key, entry.Key) {
			t.Errorf("entry[%d].Key = %s, want %s", i, got[i].Key, entry.Key)
		}
		if !bytes.Equal(got[i].Value, entry.Value) {
			t.Errorf("entry[%d].Value = %s, want %s", i, got[i].Value, entry.Value)
		}
		if got[i].Tombstone != entry.Tombstone {
			t.Errorf("entry[%d].Tombstone = %v, want %v", i, got[i].Tombstone, entry.Tombstone)
		}
	}
}

func TestWAL_Close(t *testing.T) {
	w := &WAL{
		file: NewInMemoryWalFile(),
		pool: util.NewBytesBufferPool(),
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	// Double close should return an error (file is nil)
	if err := w.Close(); err == nil {
		t.Fatal("expected error on double Close()")
	}
}

func TestWAL_Close_WriteAfterClose(t *testing.T) {
	w := &WAL{
		file: NewInMemoryWalFile(),
		pool: util.NewBytesBufferPool(),
	}

	w.Close()

	err := w.Write(&util.Entry{Key: []byte("k"), Value: []byte("v")})
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
		{
			name:     "equal versions",
			version:  "20250101120000-123456",
			other:    "20250101120000-123456",
			expected: 0,
		},
		{
			name:     "this version is less (timestamp)",
			version:  "20250101110000-123456",
			other:    "20250101120000-123456",
			expected: -1,
		},
		{
			name:     "this version is greater (timestamp)",
			version:  "20250101130000-123456",
			other:    "20250101120000-123456",
			expected: 1,
		},
		{
			name:     "this version is less (nanoseconds)",
			version:  "20250101120000-100000",
			other:    "20250101120000-200000",
			expected: -1,
		},
		{
			name:     "this version is greater (nanoseconds)",
			version:  "20250101120000-300000",
			other:    "20250101120000-200000",
			expected: 1,
		},
		{
			name:     "malformed version returns 0",
			version:  "bad",
			other:    "20250101120000-123456",
			expected: 0,
		},
		{
			name:     "malformed other returns 0",
			version:  "20250101120000-123456",
			other:    "bad",
			expected: 0,
		},
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
		{
			name:    "valid filename",
			input:   "wal-20250101120000-123456789.log",
			want:    "20250101120000-123456789",
			wantErr: false,
		},
		{
			name:    "invalid prefix",
			input:   "notwal-20250101120000-123456789.log",
			want:    "",
			wantErr: true,
		},
		{
			name:    "invalid extension",
			input:   "wal-20250101120000-123456789.txt",
			want:    "",
			wantErr: true,
		},
		{
			name:    "missing nanoseconds",
			input:   "wal-20250101120000.log",
			want:    "",
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			want:    "",
			wantErr: true,
		},
		{
			name:    "non-numeric parts",
			input:   "wal-abc-def.log",
			want:    "",
			wantErr: true,
		},
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
	err := n.Write(&util.Entry{Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatalf("NoopWAL.Write() returned error: %v", err)
	}
}

func TestNoopWAL_CompareVersion(t *testing.T) {
	n := &NoopWAL{}
	got := n.CompareVersion("20250101120000-123456")
	if got != 1 {
		t.Fatalf("NoopWAL.CompareVersion() = %d, want 1", got)
	}
}

func TestWAL_Delete_InMemory(t *testing.T) {
	// Test that Delete closes the file and sets it to nil
	w := &WAL{
		file: NewInMemoryWalFile(),
		pool: util.NewBytesBufferPool(),
		path: "/nonexistent/path/for/test.log",
	}

	// Delete will close the file but fail on os.Remove for a fake path
	err := w.Delete()
	if err == nil {
		t.Fatal("expected error from os.Remove on nonexistent path")
	}
	// But the file should have been closed (set to nil)
	if w.file != nil {
		t.Fatal("expected file to be nil after Delete()")
	}

	// Verify error message mentions the path
	if !strings.Contains(err.Error(), "test.log") {
		t.Errorf("expected error to mention file path, got: %v", err)
	}
}
