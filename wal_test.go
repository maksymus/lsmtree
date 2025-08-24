package main

import (
	"bytes"
	"testing"
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
		entries []*Entry
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
				entries: []*Entry{
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
				entries: []*Entry{
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
				entries: []*Entry{},
			},
			wantErr: false,
		},
		{
			name: "write nil entry",
			fields: fields{
				file: NewInMemoryWalFile(),
			},
			args: args{
				entries: []*Entry{nil},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &WAL{
				file: tt.fields.file,
				pool: NewBytesBufferPool(),
			}
			if err := w.Write(tt.args.entries...); (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}

			walFile := w.file.(*InMemoryWalFile)
			t.Logf("WAL buffer length: %d", walFile.buffer.Len())
		})
	}
}
