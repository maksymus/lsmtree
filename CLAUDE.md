# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Go implementation of a Log-Structured Merge Tree (LSM Tree) for key-value storage. The module path is `github.com/maksymus/lmstree` (note: `lmstree` not `lsmtree`). Requires Go 1.24. The only external dependency is `github.com/spaolacci/murmur3` for the bloom filter.

## Build & Test Commands

```bash
go build ./...                    # Build all packages
go test ./...                     # Run all tests
go test -v ./sstable              # Run tests for a specific package
go test -run TestDataBlock ./sstable  # Run a single test
go test -bench=. ./util           # Run benchmarks for util package
go test -bench=. ./sstable        # Run benchmarks for sstable package
go test -benchmem -bench=. ./...  # Benchmarks with allocation reporting
```

## Architecture

The LSM tree follows this layered design:

```
Memory:  MemTable (SkipList + WAL)
Disk:    WAL | Level 1 SSTables
              | Level 2 SSTables
              | Level 3 SSTables
```

### Packages

- **Root package (`lmstree`)** — `MemTable` (memtable.go) and the top-level `LSMTree` struct (main.go/tree.go). LSMTree.Put/Get are stubbed; MemTable is functional with mutex-protected concurrent access, SkipList storage, and WAL durability.

- **`util/`** — Core data structures shared across packages:
  - `Entry` — Key/value pair with tombstone flag; used everywhere as the unit of data
  - `SkipList` — Sorted in-memory storage (O(log n) insert/search/delete)
  - `Heap[T]` — Generic binary heap with custom comparator; used for k-way merge in SSTable
  - `BloomFilter` — Probabilistic membership test using murmur3 hashing
  - `SyncPool[T]` / `BytesBufferPool` — Object pooling to reduce allocations in hot paths

- **`sstable/`** — Sorted String Table on-disk format:
  - `DataBlock` — Sorted entries with binary encoding (key_len + value_len + key + value + tombstone)
  - `IndexBlock` — Maps key ranges to DataBlock offsets for binary search lookup
  - `MetaBlock` — Metadata (creation timestamp, level)
  - `Footer` — Fixed-size trailer with offsets/lengths of Meta and Index blocks
  - `Build()` — Constructs SSTable bytes from entries, splitting into DataBlocks by size
  - `Merge()` — K-way merge of sorted entry lists using Heap; last-write-wins for duplicates, removes tombstones

- **`wal/`** — Write-Ahead Log for crash recovery:
  - WAL files named `wal-{timestamp}-{nanoseconds}.log` with version-based ordering
  - Buffered writes with 5MB flush threshold
  - `MemTable.Recover()` replays older WAL files in chronological order
  - `WalFile` interface (`io.Reader + Writer + Seeker + Closer`) decouples WAL from `*os.File` — tests use `InMemoryWalFile` (in wal_test.go) to avoid disk I/O

### Key Patterns

- **Binary encoding**: All blocks use `encoding/binary.BigEndian` consistently. Every block type implements `Encode() ([]byte, error)` and `Decode([]byte) error`.
- **Buffer pooling**: `BytesBufferPool` (via `SyncPool`) is used in SSTable building and WAL writes to reduce GC pressure.
- **Concurrency**: MemTable and WAL use `sync.Mutex` for thread safety.
- **Generics**: `Heap[T]` and `SyncPool[T]` use Go generics with comparator/reset functions.
- **Testing**: Table-driven tests, concurrent access tests (e.g., pool with 100 goroutines), mock implementations (InMemoryWalFile in wal_test.go), and separate `_bench_test.go` files with `b.ReportAllocs()`.
