# Changelog

## [Unreleased]

### Refactored
- **Project restructured into `internal/` packages** — `util/` split into `internal/bloom`, `internal/heap`, `internal/pool`, `internal/skiplist`; `memtable/`, `sstable/`, `wal/` moved to `internal/`; prevents external consumers from coupling to implementation details
- **`entry.Entry` extracted to top-level `entry/` package** — zero-dependency type importable without pulling in any internal packages
- **`tree.go` split into three files** — `options.go` (Options, DefaultOptions, constants), `lsm.go` (Open, Put, Get, Delete, Close), `tree.go` (LSMTree struct and private methods)
- **`sstable` block files consolidated** — `data.go`, `index.go`, `meta.go`, `footer.go` merged into `internal/sstable/block.go`; `sstable.go` split into `builder.go` (Build) and `merge.go` (Merge)
- **Dead `main()` moved** — root `main.go` (unreachable in a library package) replaced by `cmd/lsmtree/main.go` (`package main`)

### Added
- **Bloom filter per SSTable** — `BloomFilter.Encode()` / `bloom.Decode()` in `internal/bloom`. `Build()` constructs a 1%-FPR filter over all entry keys and stores it in `MetaBlock`. `OpenReader` decodes the filter once; `Reader.Search` checks it as a fast path before touching the index or data blocks.
- **On-demand data-block reads in `Reader`** (`internal/sstable/reader.go`) — `OpenReader` reads only the footer, index block, and meta block (bloom); `Search` fetches just the matching data block; `Entries` streams data blocks one at a time. Added `Reader.Close()`.
- **Background flush worker** (`lsm.go` / `tree.go`) — `Put`/`Delete` hold the write lock only for the memtable write + `rotateMemTable`. A `flushWorker` goroutine performs Build/write-file/OpenReader outside the lock. `Get` searches `t.immutable` so reads never miss in-flight data.

### Fixed
- **Cascading compaction** (`tree.go`) — after each `compact(level)`, the resulting level+1 SSTable is compared against a size limit (`MemTableSize × L0CompactThresh × 10^(level-1)`); if exceeded, `compact(level+1)` is called recursively, propagating data down through all levels instead of letting L1+ grow unboundedly
- **`levelSizeLimit` / `levelSize`** (`tree.go`) — helpers used by the cascade logic; `levelSizeLimit` computes the per-level byte budget (10× multiplier per level), `levelSize` sums on-disk sizes via `os.Stat`
- **`TestLSMTree_CascadeCompaction`** (`tree_test.go`) — verifies that data is pushed to level ≥ 2 and all keys remain readable after multiple cascading compactions
- **`TestLSMTree_CloseFlushesAndClosesWAL`** (`tree_test.go`) — reopens the tree after `Close()` and verifies all data persisted, confirming the rotated WAL is properly closed

### Fixed
- **`Close()` WAL file descriptor leak** (`lsm.go`) — after `flush()` the active WAL is rotated to a new instance; the original code returned early and never called `Close()` on the new WAL. Fixed so `t.wal.Close()` is always reached.
- **`sstLevel` fragile `"0"` workaround** (`tree.go`) — `strings.TrimLeft(m[1], "0")` converts `"0"` to `""`, causing `strconv.Atoi` to fail. Replaced with a direct `strconv.Atoi(m[1])` call.
- **`Delete` not counted toward `MemTableSize`** (`internal/memtable/memtable.go`) — tombstone entries were never added to `m.size`. Fixed by adding `m.size += int64(len(key) + 1)` in `Delete`.
- **SSTable not fsynced before WAL deletion** (`tree.go`) — `writeSSTFile` now calls `f.Sync()` before `f.Close()`.
- **`SkipList.InsertEntry` stale-read on overwrite** (`internal/skiplist/skiplist.go`) — rewritten using the standard `update[]` predecessor array; existing nodes are updated in place instead of inserting duplicates.

---

### Added
- **`LSMTree` implementation** (`lsm.go`, `tree.go`) — `Open`, `Put`, `Get`, `Delete`, `Close` with configurable `Options` (MemTableSize, BlockSize, L0CompactThresh, MaxLevels)
- **`sstable.Reader`** (`internal/sstable/reader.go`) — loads an SSTable file into memory; exposes `Search(key)` and `Entries()` (full scan for compaction)
- **`SkipList.InsertEntry`**, **`GetEntry`**, **`Entries`** (`internal/skiplist/skiplist.go`) — full `Entry` support including tombstone flag; `Entries()` deduplicates by key
- **`MemTable.GetEntry`**, **`Delete`**, **`Size`**, **`Entries`** (`internal/memtable/memtable.go`) — tombstone-aware deletion, byte-size tracking, and sorted entry export for flushing
- **Integration tests** (`tree_test.go`) — Put/Get, overwrite, delete, flush-to-SSTable, and compaction with tombstone shadowing

### Fixed
- **Pool use-after-free in SSTable encoding** — `DataBlock.Encode`, `IndexBlock.Encode`, `MetaBlock.Encode`, and `Footer.Encode` returned a slice backed by a pooled `bytes.Buffer`. The deferred pool `Put` recycled the backing array before the caller was done with it. Each `Encode` now returns an independent copy via `bytes.Clone`.
- **Tombstone shadowing in `Merge`** (`internal/sstable/merge.go`) — a separate `lastKey` variable now correctly shadows all lower-priority duplicates, including tombstones.
- **WAL validation rejected tombstone entries** (`internal/wal/wal.go`) — `Write` returned an error for entries with an empty value. The check now allows empty values when `Tombstone` is true.
- **`MemTable.Recover` missing directory prefix** (`internal/memtable/memtable.go`) — WAL files were opened by filename only. Fixed to use `filepath.Join(m.dir, file)`.
