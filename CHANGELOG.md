# Changelog

## [Unreleased]

### Added
- **Cascading compaction** (`tree.go`) — after each `compact(level)`, the resulting level+1 SSTable is compared against a size limit (`MemTableSize × L0CompactThresh × 10^(level-1)`); if exceeded, `compact(level+1)` is called recursively, propagating data down through all levels instead of letting L1+ grow unboundedly
- **`levelSizeLimit` / `levelSize`** (`tree.go`) — helpers used by the cascade logic; `levelSizeLimit` computes the per-level byte budget (10× multiplier per level), `levelSize` sums on-disk sizes via `os.Stat`
- **`TestLSMTree_CascadeCompaction`** (`tree_test.go`) — verifies that data is pushed to level ≥ 2 and all keys remain readable after multiple cascading compactions
- **`TestLSMTree_CloseFlushesAndClosesWAL`** (`tree_test.go`) — reopens the tree after `Close()` and verifies all data persisted, confirming the rotated WAL is properly closed

### Fixed
- **`Close()` WAL file descriptor leak** (`tree.go`) — after `flush()` the active WAL is rotated to a new instance; the original code returned early inside the `if` branch and never called `Close()` on the new WAL. Fixed by splitting the early-return into a two-step `if` so `t.wal.Close()` is always reached.
- **`sstLevel` fragile `"0"` workaround** (`tree.go`) — `strings.TrimLeft(m[1], "0")` converts `"0"` to `""`, causing `strconv.Atoi` to fail and fall through an error-path that coincidentally returned the correct answer. Replaced with a direct `strconv.Atoi(m[1])` call; the error path now returns `false` instead of silently succeeding.
- **`Delete` not counted toward `MemTableSize`** (`memtable/memtable.go`) — tombstone entries were never added to `m.size`, so a delete-heavy workload could accumulate entries in memory without ever triggering a flush. Fixed by adding `m.size += int64(len(key) + 1)` in `Delete`.
- **SSTable not fsynced before WAL deletion** (`tree.go`) — `writeSSTFile` now calls `f.Sync()` before `f.Close()`, ensuring the SSTable bytes reach disk before `oldWAL.Delete()` runs and the WAL is gone.
- **`SkipList.InsertEntry` stale-read on overwrite** (`util/skiplist.go`) — the previous implementation always inserted a new node at a random level; if the first write for a key landed at level 3 and the second at level 0, a search starting from level 3 would return the old value. Rewritten using the standard `update[]` predecessor array: the key is checked at level 0 (which contains every node) first, and if found the existing node is updated in place; a new node is only allocated for genuinely new keys, eliminating duplicate nodes entirely

---

### Added
- **`LSMTree` implementation** (`tree.go`) — `Open`, `Put`, `Get`, `Delete`, `Close` with configurable `Options` (MemTableSize, BlockSize, L0CompactThresh, MaxLevels)
- **`sstable.Reader`** (`sstable/reader.go`) — loads an SSTable file into memory; exposes `Search(key)` (index → data block lookup) and `Entries()` (full scan for compaction)
- **`SkipList.InsertEntry`**, **`GetEntry`**, **`Entries`** (`util/skiplist.go`) — full `Entry` support including tombstone flag; `Entries()` deduplicates by key, returning the newest value
- **`MemTable.GetEntry`**, **`Delete`**, **`Size`**, **`Entries`** (`memtable/memtable.go`) — tombstone-aware deletion, byte-size tracking, and sorted entry export for flushing
- **Integration tests** (`tree_test.go`) — Put/Get, overwrite, delete, flush-to-SSTable, and compaction with tombstone shadowing

### Fixed
- **Pool use-after-free in SSTable encoding** — `DataBlock.Encode`, `IndexBlock.Encode`, `MetaBlock.Encode`, and `Footer.Encode` returned a slice backed by a pooled `bytes.Buffer`. When `Build` called multiple `Encode` functions in sequence, the deferred pool `Put` recycled the backing array before the caller finished with the earlier slice, corrupting SSTable data. Each `Encode` now returns an independent copy.
- **Tombstone shadowing in `Merge`** (`sstable/sstable.go`) — the merge loop tracked the last emitted key only through the result slice, so a tombstone (which is not appended) did not prevent a lower-priority live entry with the same key from being added. A separate `lastKey` variable now correctly shadows all lower-priority duplicates.
- **WAL validation rejected tombstone entries** (`wal/wal.go`) — `Write` returned an error for entries with an empty value, making `Delete` impossible. The check now allows empty values when `Tombstone` is true.
- **`MemTable.Recover` missing directory prefix** (`memtable/memtable.go`) — WAL files were opened by filename only, failing unless the process working directory matched the data directory. Fixed to use `filepath.Join(m.dir, file)`.
