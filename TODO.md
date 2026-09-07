# TODO

## Bugs

### ~~`Close()` leaks WAL file descriptor after flush~~ ✅

`flush()` rotates `t.wal` to a new WAL. Fixed by splitting the early-return into a
two-step `if`: flush first, then unconditionally call `t.wal.Close()` so the rotated
WAL is always closed. Covered by `TestLSMTree_CloseFlushesAndClosesWAL`.

---

### ~~`sstLevel` workaround for `"0"` is fragile~~ ✅

Replaced `strconv.Atoi(strings.TrimLeft(m[1], "0"))` with `strconv.Atoi(m[1])`.
`strconv.Atoi` handles `"0"` and leading zeros correctly on its own; the error path
now returns `false` instead of silently succeeding via an exception.

---

## Correctness / Durability

### ~~No cascading compaction — levels beyond L1 grow unboundedly~~ ✅

Implemented in `tree.go`: `levelSizeLimit` (base = `MemTableSize × L0CompactThresh`,
×10 per level), `levelSize` (sum of `os.Stat` sizes), and a cascade call at the end of
`compact`. Also fixed a pre-existing `SkipList.InsertEntry` bug where a duplicate node
at a lower random level caused stale reads. Covered by `TestLSMTree_CascadeCompaction`.

---

### ~~SSTable not fsynced before WAL deletion~~ ✅

`writeSSTFile` now uses `os.OpenFile` + `f.Write` + `f.Sync()` + `f.Close()` instead
of `os.WriteFile`, guaranteeing the SSTable is on disk before `oldWAL.Delete()` runs.

---

### ~~`Delete` does not contribute to `MemTableSize`~~ ✅

`MemTable.Delete` now increments `m.size += int64(len(key) + 1)` so tombstones count
toward the flush threshold and delete-heavy workloads still trigger flushes.

---

## Performance

### ~~Bloom filters exist but are unused~~ ✅

Added `BloomFilter.Encode()` and `bloom.Decode()` in `internal/bloom`. `Build()` now
creates a filter at 1% FPR, adds every entry key, and stores the serialized bytes in
`MetaBlock` (format: `createdAt | level | bloomLen | bloom bits`). `OpenReader` decodes
the filter once and caches it in `Reader.bloom`. `Search` checks the filter first and
returns `nil, false` immediately for any key that is definitely absent.

---

### ~~`Reader` loads the entire SSTable into memory~~ ✅

`internal/sstable.Reader` holds an `*os.File` instead of `[]byte`. `OpenReader` reads
only the footer (32 bytes via `ReadAt`), index block, and meta block at open time.
`Search` fetches only the matching data block on demand; `Entries` fetches each data
block in turn. Added `Reader.Close()`. `LSMTree.Close()` closes all level readers;
`compact()` closes readers before deleting their files.

---

### ~~Flush and compaction hold the global write lock~~ ✅

`Put`/`Delete` now hold the write lock only for the memtable write + atomic rotation
(`rotateMemTable`). A background `flushWorker` goroutine performs the heavy I/O
(Build SST, write file, OpenReader) without holding the lock. The lock is re-acquired
briefly only to install the result into `t.levels` and clear `t.immutable`. `Get`
also checks `t.immutable` (the in-flight memtable) so reads never miss data.
`Close` stops the worker, drains any pending job, and flushes the active memtable
synchronously before returning.

---

## Missing Functionality

### ~~No range scan / iterator~~ ✅

Implemented across three layers:

- `SkipList.Range(start, end)` / `MemTable.Range(start, end)` — snapshot of the
  entries in `[start, end)` in sorted order, tombstones included. Taken under the
  MemTable mutex so a concurrent writer cannot corrupt the walk.
- `internal/sstable.Reader.Iterator(start, end)` — ordered scan of one SSTable that
  fetches a single data block at a time. Index entries whose range falls entirely
  outside the bounds are skipped without any read.
- `LSMTree.Scan(start, end)` (`iterator.go`) — k-way merge via `internal/heap` over
  the MemTable, the immutable MemTable, and every SSTable level. Sources are seeded
  newest-first (same order as `Get`); ties on a key go to the lowest priority index,
  and every shadowed copy plus every tombstoned key is dropped.

A `nil` bound is unbounded, so `Scan(nil, nil)` walks the whole tree. `Next` returns
`(*entry.Entry, bool)`, `Err` reports the first read failure, and `Close` must be
called to unpin SSTables.

To keep a live iterator safe against a concurrent compaction, `sstableFile` became
reference-counted: the tree holds one reference while the file is in `t.levels`, each
iterator holds one more, and `compact` marks superseded files obsolete and releases
them rather than closing and unlinking outright. The last release does the cleanup.

Also added a `scan [start] [end]` REPL command (`-` for an unbounded side).

---

### No manifest file — directory scan is fragile

`loadSSTables` admits any file matching the naming pattern. A crash mid-compaction
(new SST written, old ones not yet deleted) leaves orphaned files that get loaded,
producing duplicate data. A small append-only manifest makes the live set authoritative.

```go
// manifest.go
type ManifestEntry struct {
    Op    string // "add" | "remove"
    Level int
    Path  string
}

func writeManifest(dir string, entries []ManifestEntry) error {
    path := filepath.Join(dir, "MANIFEST")
    f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return err
    }
    defer f.Close()
    enc := json.NewEncoder(f)
    for _, e := range entries {
        if err := enc.Encode(e); err != nil {
            return err
        }
    }
    return f.Sync()
}

// Usage in compact(): write manifest entries atomically before deleting old files.
// On Open(), replay the manifest instead of scanning the directory.
```
