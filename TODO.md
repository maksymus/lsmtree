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

### Bloom filters exist but are unused

`util.BloomFilter` is already implemented. Build one per SSTable, serialise it alongside
the MetaBlock, and check it in `Reader.Search` before touching the index or data blocks.
This turns most negative lookups into a single in-memory bitset check.

```go
// sstable/sstable.go — build phase
bloom := util.NewBloomFilter(len(entries), 0.01)
for _, e := range entries {
    bloom.Add(e.Key)
}
bloomBytes, err := bloom.Encode() // needs Encode/Decode added to BloomFilter
// serialise bloomBytes into MetaBlock alongside createdAt and level

// sstable/reader.go — lookup phase
func (r *Reader) Search(key []byte) (*util.Entry, bool) {
    // fast path: bloom filter rules out keys that are definitely absent
    if r.bloom != nil && !r.bloom.Contains(key) {
        return nil, false
    }
    // ... existing index → data block path ...
}
```

---

### `Reader` loads the entire SSTable into memory

`os.ReadFile` reads every byte upfront. Only the footer and index block are needed at
open time; the matching data block should be fetched on demand.

```go
// sstable/reader.go
type Reader struct {
    f     *os.File
    size  int64
    index *IndexBlock // loaded once at open
    bloom *util.BloomFilter
}

func OpenReader(path string) (*Reader, error) {
    f, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    info, err := f.Stat()
    if err != nil {
        f.Close()
        return nil, err
    }
    r := &Reader{f: f, size: info.Size()}

    // Read and decode footer + index block only.
    footerBuf := make([]byte, footerSize)
    if _, err := f.ReadAt(footerBuf, info.Size()-footerSize); err != nil {
        f.Close()
        return nil, err
    }
    footer := &Footer{}
    if err := footer.Decode(footerBuf); err != nil {
        f.Close()
        return nil, err
    }
    indexBuf := make([]byte, footer.index.length)
    if _, err := f.ReadAt(indexBuf, int64(footer.index.offset)); err != nil {
        f.Close()
        return nil, err
    }
    r.index = &IndexBlock{}
    if err := r.index.Decode(indexBuf); err != nil {
        f.Close()
        return nil, err
    }
    return r, nil
}

func (r *Reader) Search(key []byte) (*util.Entry, bool) {
    block, found := r.index.Search(key)
    if !found {
        return nil, false
    }
    buf := make([]byte, block.length)
    if _, err := r.f.ReadAt(buf, int64(block.offset)); err != nil {
        return nil, false
    }
    db := &DataBlock{}
    if err := db.Decode(buf); err != nil {
        return nil, false
    }
    return db.Search(key)
}

func (r *Reader) Close() error { return r.f.Close() }
```

---

### Flush and compaction hold the global write lock

`Put` holds `t.mu.Lock()` for the full duration of `flush()`, blocking all concurrent
reads and writes during file I/O. Rotate the MemTable pointer atomically (fast), then
flush the old MemTable in a background goroutine.

```go
// tree.go
type LSMTree struct {
    mu        sync.RWMutex
    opts      Options
    memTable  *memtable.MemTable
    immutable *memtable.MemTable // being flushed; checked by Get
    wal       *walPkg.WAL
    levels    [][]*sstableFile
    flushCh   chan *memtable.MemTable
    done      chan struct{}
}

func Open(opts Options) (*LSMTree, error) {
    // ... existing setup ...
    t.flushCh = make(chan *memtable.MemTable, 1)
    t.done = make(chan struct{})
    go t.flushWorker()
    return t, nil
}

func (t *LSMTree) flushWorker() {
    for {
        select {
        case mem := <-t.flushCh:
            t.mu.Lock()
            _ = t.flushMemTable(mem) // write SST, rotate WAL, maybe compact
            t.immutable = nil
            t.mu.Unlock()
        case <-t.done:
            return
        }
    }
}

func (t *LSMTree) Put(key, value []byte) error {
    t.mu.Lock()
    defer t.mu.Unlock()

    if err := t.memTable.Set(key, value); err != nil {
        return err
    }
    if t.memTable.Size() >= t.opts.MemTableSize && t.immutable == nil {
        t.immutable = t.memTable
        newWAL, _ := walPkg.Create(t.opts.Dir)
        t.memTable = memtable.NewMemTable(t.opts.Dir, defaultSkipListLevel, newWAL)
        t.wal = newWAL
        t.flushCh <- t.immutable // hand off; lock released before actual I/O
    }
    return nil
}

func (t *LSMTree) Get(key []byte) ([]byte, bool) {
    t.mu.RLock()
    defer t.mu.RUnlock()

    if entry, ok := t.memTable.GetEntry(key); ok {
        if entry.Tombstone {
            return nil, false
        }
        return entry.Value, true
    }
    // also check the immutable memtable being flushed
    if t.immutable != nil {
        if entry, ok := t.immutable.GetEntry(key); ok {
            if entry.Tombstone {
                return nil, false
            }
            return entry.Value, true
        }
    }
    // ... SSTable search unchanged ...
    return nil, false
}
```

---

## Missing Functionality

### No range scan / iterator

Add a merged iterator over the MemTable and all SSTable levels. The SkipList level-0
list supports ordered traversal; SSTable data blocks are already sorted. A k-way merge
using `util.Heap` (already used in `sstable.Merge`) exposes full ordered scan.

```go
// iterator.go
type Iterator struct {
    heap *util.Heap[iterItem]
}

type iterItem struct {
    entry     *util.Entry
    advance   func() (*util.Entry, bool) // next() for this source
}

func (t *LSMTree) Scan(start, end []byte) *Iterator {
    // seed one iterItem per source: memtable, each L0 SST, each L1+ SST
    // heap orders by key; equal keys resolved newest-source-first (same as Merge)
}

func (it *Iterator) Next() (*util.Entry, bool) {
    if it.heap.Len() == 0 {
        return nil, false
    }
    item, _ := it.heap.Pop()
    if next, ok := item.advance(); ok {
        it.heap.Push(iterItem{entry: next, advance: item.advance})
    }
    if item.entry.Tombstone {
        return it.Next() // skip tombstones
    }
    return item.entry, true
}
```

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
