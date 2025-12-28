package sstable

import (
	"testing"

	"github.com/maksymus/lmstree/util"
)

func BenchmarkMetaBlock_EncodeDecode(b *testing.B) {
	meta := &MetaBlock{
		createdAt: 1625077800,
		level:     2,
	}

	b.Run("Encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := meta.Encode(); err != nil {
				b.Fatalf("Encode failed: %v", err)
			}
		}
	})

	data, _ := meta.Encode()
	b.Run("Decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := meta.Decode(data); err != nil {
				b.Fatalf("Decode failed: %v", err)
			}
		}
	})
}

func BenchmarkFooter_EncodeDecode(b *testing.B) {
	footer := &Footer{
		meta: Block{
			offset: 0,
			length: 128,
		},
		index: Block{
			offset: 128,
			length: 256,
		},
	}

	b.Run("Encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := footer.Encode(); err != nil {
				b.Fatalf("Encode failed: %v", err)
			}
		}
	})

	data, _ := footer.Encode()
	b.Run("Decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := footer.Decode(data); err != nil {
				b.Fatalf("Decode failed: %v", err)
			}
		}
	})
}

func BenchmarkIndexBlock_EncodeDecode(b *testing.B) {
	indexBlock := &IndexBlock{
		entries: []*IndexEntry{
			{
				startKey: []byte("a"),
				endKey:   []byte("m"),
				block: Block{
					offset: 0,
					length: 128,
				},
			},
			{
				startKey: []byte("n"),
				endKey:   []byte("z"),
				block: Block{
					offset: 128,
					length: 256,
				},
			},
		},
	}

	b.Run("Encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := indexBlock.Encode(); err != nil {
				b.Fatalf("Encode failed: %v", err)
			}
		}
	})

	data, _ := indexBlock.Encode()
	b.Run("Decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := indexBlock.Decode(data); err != nil {
				b.Fatalf("Decode failed: %v", err)
			}
		}
	})
}

func BenchmarkDataBlock_EncodeDecode(b *testing.B) {
	dataBlock := &DataBlock{
		entries: []*util.Entry{
			{Key: []byte("apple"), Value: []byte("fruit")},
			{Key: []byte("carrot"), Value: []byte("vegetable")},
			{Key: []byte("banana"), Value: []byte("fruit")},
		},
	}

	b.Run("Encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := dataBlock.Encode(); err != nil {
				b.Fatalf("Encode failed: %v", err)
			}
		}
	})

	data, _ := dataBlock.Encode()
	b.Run("Decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := dataBlock.Decode(data); err != nil {
				b.Fatalf("Decode failed: %v", err)
			}
		}
	})
}
