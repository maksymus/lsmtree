# lsmtree

A Go implementation of a Log-Structured Merge Tree (LSM Tree) for persistent key-value storage.

## Features

- **Leveled compaction** with cascading — L0 → L1 → LN triggered automatically by size thresholds
- **Bloom filters** per SSTable — fast negative lookups skip disk reads entirely
- **On-demand data-block reads** — only footer, index, and bloom filter loaded at open time
- **Background flush worker** — `Put`/`Delete` hold the write lock only for the in-memory write; heavy I/O runs concurrently
- **Write-Ahead Log** — crash recovery by replaying WAL files on `Open`
- **Tombstone-aware delete** — deletions shadow older values through compaction

## Usage

```go
import lmstree "github.com/maksymus/lmstree"

tree, err := lmstree.Open(lmstree.DefaultOptions("/path/to/data"))
if err != nil {
    log.Fatal(err)
}
defer tree.Close()

tree.Put([]byte("hello"), []byte("world"))

val, ok := tree.Get([]byte("hello"))

tree.Delete([]byte("hello"))
```

### Options

```go
opts := lmstree.Options{
    Dir:             "/path/to/data",
    MemTableSize:    64 * 1024 * 1024, // 64 MB — flush threshold
    BlockSize:       4096,             // SSTable data-block size
    L0CompactThresh: 4,                // L0 files before compaction
    MaxLevels:       7,
}
```

## Architecture

```
Memory:  active MemTable  (SkipList + WAL)
         immutable MemTable (being flushed by background worker)
Disk:    WAL
         Level 0 SSTables  (unsorted, may overlap)
         Level 1 SSTables  (merged, sorted)
         ...
         Level N SSTables
```

### Package layout

```
lsmtree/
├── options.go              # Options, DefaultOptions
├── lsm.go                  # Open, Put, Get, Delete, Close
├── tree.go                 # LSMTree struct + private methods
├── entry/                  # Entry{Key, Value, Tombstone} — zero deps
├── cmd/lsmtree/            # demo CLI (package main)
└── internal/
    ├── bloom/              # BloomFilter with murmur3 hashing
    ├── heap/               # generic Heap[T] for k-way merge
    ├── pool/               # SyncPool[T] / BytesBufferPool
    ├── skiplist/           # sorted SkipList with tombstone support
    ├── memtable/           # MemTable (SkipList + WAL, mutex-protected)
    ├── sstable/
    │   ├── block.go        # DataBlock, IndexBlock, MetaBlock, Footer
    │   ├── builder.go      # Build() — constructs SSTable bytes
    │   ├── merge.go        # Merge() — k-way merge, last-write-wins
    │   └── reader.go       # Reader — on-demand block reads
    └── wal/                # Write-Ahead Log + NoopWAL
```

### SSTable format

```
+-------------------+
| Data Block 1      |  sorted entries: key_len(4) | val_len(4) | key | val | tombstone(1)
+-------------------+
| Data Block ...    |
+-------------------+
| Meta Block        |  createdAt(8) | level(4) | bloomLen(4) | bloom bits
+-------------------+
| Index Block       |  per data block: startKey | endKey | offset(8) | length(8)
+-------------------+
| Footer (32 bytes) |  meta.offset(8) | meta.len(8) | index.offset(8) | index.len(8)
+-------------------+
```

## Building & Testing

```bash
go build ./...
go test ./...
go test -race ./...
go test -bench=. -benchmem ./...
```
