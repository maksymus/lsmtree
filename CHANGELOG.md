# Changelog

## [Unreleased]

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
