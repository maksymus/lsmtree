# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Go implementation of a Log-Structured Merge Tree (LSM Tree) for key-value storage. The module path is `github.com/maksymus/lmstree` (note: `lmstree` not `lsmtree`). Requires Go 1.24. The only external dependency is `github.com/spaolacci/murmur3` for the bloom filter.

## Build & Test Commands

```bash
go build ./...                              # Build all packages
go test ./...                              # Run all tests
go test -v ./internal/sstable              # Run tests for a specific package
go test -run TestDataBlock ./internal/sstable  # Run a single test
go test -bench=. ./internal/heap           # Run benchmarks for heap package
go test -bench=. ./internal/sstable        # Run benchmarks for sstable package
go test -benchmem -bench=. ./...           # Benchmarks with allocation reporting
```

## Architecture

The LSM tree follows this layered design:

```
Memory:  MemTable (SkipList + WAL)
Disk:    WAL | Level 0 SSTables
              | Level 1 SSTables
              | Level N SSTables
```

### Packages

- **Root package (`lmstree`)** — public API split across three files:
  - `options.go` — `Options`, `DefaultOptions`, shared constants
  - `lsm.go` — `Open`, `Put`, `Get`, `Delete`, `Close`
  - `tree.go` — `LSMTree` struct, internal types (`sstableFile`, `flushJob`), and all private methods (flush, compact, loadSSTables, etc.)

- **`entry/`** — `Entry` struct (Key, Value, Tombstone + Size()). Zero internal dependencies; importable by any package including external consumers.

- **`cmd/lsmtree/`** — `package main` demo/CLI entry point.

- **`internal/bloom/`** — `BloomFilter` with murmur3 hashing, `Encode()` / `Decode()` for serialization into SSTable MetaBlock.

- **`internal/heap/`** — Generic `Heap[T]` wrapping `container/heap`; used for k-way merge in SSTable and (future) iterators.

- **`internal/pool/`** — `SyncPool[T]` / `BytesBufferPool` — object pooling to reduce allocations in hot paths (SSTable building, WAL writes).

- **`internal/skiplist/`** — `SkipList` with `InsertEntry` / `GetEntry` / `Entries()` for tombstone-aware sorted storage.

- **`internal/memtable/`** — `MemTable` (mutex-protected, SkipList-backed, WAL-durable). `Recover()` replays older WAL files on startup.

- **`internal/sstable/`** — Sorted String Table on-disk format, split across files:
  - `block.go` — `DataBlock`, `IndexBlock`, `MetaBlock`, `Footer`, `Block` handle, shared `bytesBufPool`
  - `builder.go` — `Build()`: constructs SSTable bytes from entries, splits into DataBlocks by size, embeds bloom filter in MetaBlock
  - `merge.go` — `Merge()`: k-way merge via `internal/heap`; last-write-wins for duplicates, drops tombstones
  - `reader.go` — `Reader`: opens file once (footer + index + bloom), fetches data blocks on demand via `ReadAt`

- **`internal/wal/`** — Write-Ahead Log:
  - WAL files named `wal-{timestamp}-{nanoseconds}.log` with version-based ordering
  - Buffered writes with 5MB flush threshold
  - `WalFile` interface decouples WAL from `*os.File`; tests use `InMemoryWalFile`
  - `NoopWAL` for testing without durability

### Key Patterns

- **Binary encoding**: All blocks use `encoding/binary.BigEndian`. Every block type implements `Encode() ([]byte, error)` and `Decode([]byte) error`. `Encode` returns `bytes.Clone(buffer.Bytes())` to avoid pool use-after-free.
- **Buffer pooling**: `BytesBufferPool` (via `SyncPool`) is used in SSTable building and WAL writes to reduce GC pressure.
- **Concurrency**: `LSMTree` uses `sync.RWMutex`; MemTable and WAL use `sync.Mutex`. Background flush worker communicates via a buffered channel (`flushCh`, capacity 1).
- **Background flush**: `Put`/`Delete` hold the write lock only for the memtable write + `rotateMemTable`. The `flushWorker` goroutine does all I/O without the lock.
- **Generics**: `Heap[T]` and `SyncPool[T]` use Go generics.
- **Testing**: Table-driven tests, concurrent access tests, `InMemoryWalFile` mock, separate `_bench_test.go` files with `b.ReportAllocs()`.
