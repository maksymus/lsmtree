package lmstree

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/maksymus/lmstree/entry"
	"github.com/maksymus/lmstree/internal/memtable"
	"github.com/maksymus/lmstree/internal/sstable"
	walPkg "github.com/maksymus/lmstree/internal/wal"
)

const (
	defaultMemTableSize    int64 = 64 * 1024 * 1024 // 64 MB
	defaultBlockSize       int   = 4096
	defaultL0CompactThresh int   = 4
	defaultMaxLevels       int   = 7
	defaultSkipListLevel   int   = 16
)

// Options configures the LSMTree.
type Options struct {
	Dir             string // directory for WAL and SSTable files
	MemTableSize    int64  // memtable size in bytes before a flush is triggered
	BlockSize       int    // target SSTable data-block size in bytes
	L0CompactThresh int    // number of L0 SSTables that triggers a compaction to L1
	MaxLevels       int    // maximum number of levels
}

// DefaultOptions returns sensible defaults for the given directory.
func DefaultOptions(dir string) Options {
	return Options{
		Dir:             dir,
		MemTableSize:    defaultMemTableSize,
		BlockSize:       defaultBlockSize,
		L0CompactThresh: defaultL0CompactThresh,
		MaxLevels:       defaultMaxLevels,
	}
}

// sstableFile is an in-memory handle for one SSTable file on disk.
type sstableFile struct {
	path   string
	level  int
	reader *sstable.Reader
}

// flushJob carries a frozen MemTable and its WAL to the background flush worker.
type flushJob struct {
	mem    *memtable.MemTable
	oldWAL *walPkg.WAL
}

// LSMTree is a Log-Structured Merge Tree backed by a MemTable and leveled SSTables.
//
//	Memory:  active MemTable (SkipList + WAL)
//	         immutable MemTable (being flushed by background worker)
//	Disk:    WAL | Level 0 SSTables  (unsorted, may overlap)
//	              | Level 1 SSTables
//	              | Level N SSTables  (merged, no overlap within a level)
type LSMTree struct {
	mu        sync.RWMutex
	opts      Options
	memTable  *memtable.MemTable
	immutable *memtable.MemTable // frozen; being flushed by flushWorker
	wal       *walPkg.WAL        // current active WAL
	levels    [][]*sstableFile   // levels[i] = SSTables at level i, newest first
	flushCh   chan flushJob       // capacity 1; at most one flush in flight at a time
	done      chan struct{}       // closed by Close() to stop the worker
	wg        sync.WaitGroup     // tracks the flush worker goroutine
}

// Open creates or opens the LSMTree rooted at opts.Dir.
// Any WAL files left from a previous crash are replayed into the MemTable.
func Open(opts Options) (*LSMTree, error) {
	if opts.MemTableSize == 0 {
		opts.MemTableSize = defaultMemTableSize
	}
	if opts.BlockSize == 0 {
		opts.BlockSize = defaultBlockSize
	}
	if opts.L0CompactThresh == 0 {
		opts.L0CompactThresh = defaultL0CompactThresh
	}
	if opts.MaxLevels == 0 {
		opts.MaxLevels = defaultMaxLevels
	}

	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, err
	}

	w, err := walPkg.Create(opts.Dir)
	if err != nil {
		return nil, err
	}

	mem := memtable.NewMemTable(opts.Dir, defaultSkipListLevel, w)

	t := &LSMTree{
		opts:     opts,
		memTable: mem,
		wal:      w,
		levels:   make([][]*sstableFile, opts.MaxLevels),
		flushCh:  make(chan flushJob, 1),
		done:     make(chan struct{}),
	}

	// Replay any WAL files from a previous crash.
	if err := mem.Recover(); err != nil {
		return nil, err
	}

	// Load existing SSTable files from disk.
	if err := t.loadSSTables(); err != nil {
		return nil, err
	}

	t.wg.Add(1)
	go t.flushWorker()

	return t, nil
}

// Put stores key → value. If the MemTable exceeds MemTableSize and no flush is
// already in progress, it is rotated out to the background flush worker.
func (t *LSMTree) Put(key, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.memTable.Set(key, value); err != nil {
		return err
	}
	if t.memTable.Size() >= t.opts.MemTableSize && t.immutable == nil {
		t.rotateMemTable()
	}
	return nil
}

// Delete marks key as deleted. The tombstone is written to the MemTable and will
// shadow any older value in SSTables until the next compaction removes both.
func (t *LSMTree) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.memTable.Delete(key); err != nil {
		return err
	}
	if t.memTable.Size() >= t.opts.MemTableSize && t.immutable == nil {
		t.rotateMemTable()
	}
	return nil
}

// Get returns the value for key and true if found, or nil and false if the key
// does not exist or has been deleted.
func (t *LSMTree) Get(key []byte) ([]byte, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Active MemTable has the freshest data.
	if entry, ok := t.memTable.GetEntry(key); ok {
		if entry.Tombstone {
			return nil, false
		}
		return entry.Value, true
	}

	// Immutable MemTable is older than active but newer than any SSTable.
	if t.immutable != nil {
		if entry, ok := t.immutable.GetEntry(key); ok {
			if entry.Tombstone {
				return nil, false
			}
			return entry.Value, true
		}
	}

	// Search SSTables level by level (L0 first = newest data).
	// Within L0, files are ordered newest-first so the first hit wins.
	for _, level := range t.levels {
		for _, sst := range level {
			entry, ok := sst.reader.Search(key)
			if !ok {
				continue
			}
			// First match at any level is authoritative.
			if entry.Tombstone {
				return nil, false
			}
			return entry.Value, true
		}
	}

	return nil, false
}

// Close stops the background flush worker, ensures all data is flushed to disk,
// closes all SSTable readers, and releases the WAL.
func (t *LSMTree) Close() error {
	// Signal the worker to stop and wait for any in-progress flush to finish.
	close(t.done)
	t.wg.Wait()

	// If the worker exited before picking up a pending job, process it now.
	select {
	case job := <-t.flushCh:
		t.processFlush(job)
	default:
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Flush any data remaining in the active MemTable.
	if t.memTable.Size() > 0 {
		if err := t.flush(); err != nil {
			return err
		}
	}

	for _, level := range t.levels {
		for _, sst := range level {
			sst.reader.Close()
		}
	}
	return t.wal.Close()
}

// rotateMemTable atomically swaps the active MemTable for a fresh one and hands
// the old one to the background flush worker. Must be called with t.mu held.
// The caller must have verified t.immutable == nil before calling.
func (t *LSMTree) rotateMemTable() {
	oldWAL := t.wal
	newWAL, err := walPkg.Create(t.opts.Dir)
	if err != nil {
		return // WAL creation failed; skip rotation, memtable continues to grow
	}
	t.immutable = t.memTable
	t.wal = newWAL
	t.memTable = memtable.NewMemTable(t.opts.Dir, defaultSkipListLevel, newWAL)
	t.flushCh <- flushJob{mem: t.immutable, oldWAL: oldWAL}
}

// flushWorker runs in a background goroutine and processes flush jobs one at a time.
func (t *LSMTree) flushWorker() {
	defer t.wg.Done()
	for {
		select {
		case job := <-t.flushCh:
			t.processFlush(job)
		case <-t.done:
			return
		}
	}
}

// processFlush builds an SSTable from the frozen MemTable, writes it to disk, and
// installs it into levels[0] — all without holding t.mu during the heavy I/O.
// The write lock is acquired briefly at the end to update shared state.
func (t *LSMTree) processFlush(job flushJob) {
	entries := job.mem.Entries()
	if len(entries) == 0 {
		t.mu.Lock()
		t.immutable = nil
		t.mu.Unlock()
		job.oldWAL.Delete()
		return
	}

	// Build SST bytes — reading from the frozen immutable MemTable, no lock needed.
	data, err := sstable.Build(entries, t.opts.BlockSize, 0)
	if err != nil {
		t.mu.Lock()
		t.immutable = nil
		t.mu.Unlock()
		return
	}

	// Write SST file to disk — no lock needed.
	path, err := t.writeSSTFile(0, data)
	if err != nil {
		t.mu.Lock()
		t.immutable = nil
		t.mu.Unlock()
		return
	}

	// Open reader — no lock needed.
	reader, err := sstable.OpenReader(path)
	if err != nil {
		os.Remove(path)
		t.mu.Lock()
		t.immutable = nil
		t.mu.Unlock()
		return
	}

	// Install the new SSTable and clear the immutable pointer under the write lock.
	t.mu.Lock()
	t.levels[0] = append([]*sstableFile{{path: path, level: 0, reader: reader}}, t.levels[0]...)
	t.immutable = nil
	if len(t.levels[0]) >= t.opts.L0CompactThresh {
		_ = t.compact(0)
	}
	t.mu.Unlock()

	job.oldWAL.Delete()
}

// flush is the synchronous path used only by Close(): it writes the active
// MemTable to a new L0 SSTable, rotates the WAL, and triggers compaction if needed.
// Must be called with t.mu held.
func (t *LSMTree) flush() error {
	entries := t.memTable.Entries()
	if len(entries) == 0 {
		return nil
	}

	data, err := sstable.Build(entries, t.opts.BlockSize, 0)
	if err != nil {
		return err
	}

	path, err := t.writeSSTFile(0, data)
	if err != nil {
		return err
	}

	reader, err := sstable.OpenReader(path)
	if err != nil {
		return err
	}

	// Prepend so levels[0][0] is always the newest L0 file.
	t.levels[0] = append([]*sstableFile{{path: path, level: 0, reader: reader}}, t.levels[0]...)

	// Rotate WAL: the old WAL's data is now in the SSTable.
	oldWAL := t.wal
	newWAL, err := walPkg.Create(t.opts.Dir)
	if err != nil {
		return err
	}
	t.wal = newWAL
	t.memTable = memtable.NewMemTable(t.opts.Dir, defaultSkipListLevel, newWAL)
	oldWAL.Delete() // best-effort; crash recovery handles any leftover

	// Compact L0 → L1 when the threshold is reached.
	if len(t.levels[0]) >= t.opts.L0CompactThresh {
		return t.compact(0)
	}
	return nil
}

// compact merges all SSTables at level into a single SSTable at level+1,
// then deletes the input files. Must be called with t.mu held.
func (t *LSMTree) compact(level int) error {
	if level >= t.opts.MaxLevels-1 {
		return nil
	}

	// Collect entry slices: level+1 first (lower priority), then current level
	// oldest-first (so the newest L0 file gets the highest listIndex and wins).
	var allEntries [][]*entry.Entry

	for _, sst := range t.levels[level+1] {
		entries, err := sst.reader.Entries()
		if err != nil {
			return err
		}
		allEntries = append(allEntries, entries)
	}

	for i := len(t.levels[level]) - 1; i >= 0; i-- {
		entries, err := t.levels[level][i].reader.Entries()
		if err != nil {
			return err
		}
		allEntries = append(allEntries, entries)
	}

	merged, err := sstable.Merge(allEntries...)
	if err != nil {
		return err
	}

	// Collect files to delete before overwriting the slice references.
	toDelete := make([]*sstableFile, 0, len(t.levels[level])+len(t.levels[level+1]))
	toDelete = append(toDelete, t.levels[level]...)
	toDelete = append(toDelete, t.levels[level+1]...)

	t.levels[level] = nil
	t.levels[level+1] = nil

	if len(merged) > 0 {
		data, err := sstable.Build(merged, t.opts.BlockSize, level+1)
		if err != nil {
			return err
		}

		path, err := t.writeSSTFile(level+1, data)
		if err != nil {
			return err
		}

		reader, err := sstable.OpenReader(path)
		if err != nil {
			return err
		}
		t.levels[level+1] = []*sstableFile{{path: path, level: level + 1, reader: reader}}
	}

	for _, sst := range toDelete {
		sst.reader.Close()
		os.Remove(sst.path)
	}

	// Cascade: if the newly written level+1 exceeds its size budget, compact it too.
	if len(t.levels[level+1]) > 0 && t.levelSize(level+1) > t.levelSizeLimit(level+1) {
		return t.compact(level + 1)
	}

	return nil
}

// levelSizeLimit returns the byte budget for the given level.
// Base (level 1) = MemTableSize × L0CompactThresh; each subsequent level is 10× larger.
func (t *LSMTree) levelSizeLimit(level int) int64 {
	limit := t.opts.MemTableSize * int64(t.opts.L0CompactThresh)
	for i := 1; i < level; i++ {
		limit *= 10
	}
	return limit
}

// levelSize returns the total on-disk byte size of all SSTables at the given level.
func (t *LSMTree) levelSize(level int) int64 {
	var total int64
	for _, sst := range t.levels[level] {
		if info, err := os.Stat(sst.path); err == nil {
			total += info.Size()
		}
	}
	return total
}

// writeSSTFile writes data to a new uniquely-named SSTable file for the given level.
func (t *LSMTree) writeSSTFile(level int, data []byte) (string, error) {
	now := time.Now()
	version := fmt.Sprintf("%s-%d", now.Format("20060102150405"), now.Nanosecond())
	name := fmt.Sprintf("sst-%d-%s.sst", level, version)
	path := filepath.Join(t.opts.Dir, name)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return "", err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Sync(); err != nil { // flush to disk before WAL is deleted
		f.Close()
		return "", err
	}
	return path, f.Close()
}

// loadSSTables scans opts.Dir for existing SSTable files and opens Readers for them.
func (t *LSMTree) loadSSTables() error {
	dirEntries, err := os.ReadDir(t.opts.Dir)
	if err != nil {
		return err
	}

	for _, de := range dirEntries {
		if de.IsDir() {
			continue
		}
		level, ok := sstLevel(de.Name())
		if !ok || level >= t.opts.MaxLevels {
			continue
		}

		path := filepath.Join(t.opts.Dir, de.Name())
		reader, err := sstable.OpenReader(path)
		if err != nil {
			return err
		}
		t.levels[level] = append(t.levels[level], &sstableFile{path: path, level: level, reader: reader})
	}

	// Sort each level newest-first (reverse lexicographic on filename encodes time).
	for i := range t.levels {
		lvl := t.levels[i]
		sort.Slice(lvl, func(a, b int) bool {
			return lvl[a].path > lvl[b].path
		})
	}

	return nil
}

// sstLevel parses the level from an SSTable filename "sst-{level}-{ts}-{ns}.sst".
var sstPattern = regexp.MustCompile(`^sst-(\d+)-\d+-\d+\.sst$`)

func sstLevel(name string) (int, bool) {
	m := sstPattern.FindStringSubmatch(name)
	if m == nil {
		return 0, false
	}
	level, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, false
	}
	return level, true
}
