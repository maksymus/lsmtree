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

// rotateMemTable atomically swaps the active MemTable for a fresh one and hands
// the old one to the background flush worker. Must be called with t.mu held.
func (t *LSMTree) rotateMemTable() {
	oldWAL := t.wal
	newWAL, err := walPkg.Create(t.opts.Dir)
	if err != nil {
		return
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
// installs it into levels[0] — without holding t.mu during the heavy I/O.
func (t *LSMTree) processFlush(job flushJob) {
	entries := job.mem.Entries()
	if len(entries) == 0 {
		t.mu.Lock()
		t.immutable = nil
		t.mu.Unlock()
		job.oldWAL.Delete()
		return
	}

	data, err := sstable.Build(entries, t.opts.BlockSize, 0)
	if err != nil {
		t.mu.Lock()
		t.immutable = nil
		t.mu.Unlock()
		return
	}

	path, err := t.writeSSTFile(0, data)
	if err != nil {
		t.mu.Lock()
		t.immutable = nil
		t.mu.Unlock()
		return
	}

	reader, err := sstable.OpenReader(path)
	if err != nil {
		os.Remove(path)
		t.mu.Lock()
		t.immutable = nil
		t.mu.Unlock()
		return
	}

	t.mu.Lock()
	t.levels[0] = append([]*sstableFile{{path: path, level: 0, reader: reader}}, t.levels[0]...)
	t.immutable = nil
	if len(t.levels[0]) >= t.opts.L0CompactThresh {
		_ = t.compact(0)
	}
	t.mu.Unlock()

	job.oldWAL.Delete()
}

// flush is the synchronous flush path used only by Close(). Must be called with t.mu held.
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

	t.levels[0] = append([]*sstableFile{{path: path, level: 0, reader: reader}}, t.levels[0]...)

	oldWAL := t.wal
	newWAL, err := walPkg.Create(t.opts.Dir)
	if err != nil {
		return err
	}
	t.wal = newWAL
	t.memTable = memtable.NewMemTable(t.opts.Dir, defaultSkipListLevel, newWAL)
	oldWAL.Delete()

	if len(t.levels[0]) >= t.opts.L0CompactThresh {
		return t.compact(0)
	}
	return nil
}

// compact merges all SSTables at level into a single SSTable at level+1.
// Must be called with t.mu held.
func (t *LSMTree) compact(level int) error {
	if level >= t.opts.MaxLevels-1 {
		return nil
	}

	// level+1 entries first (lower priority), then current level oldest-first.
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

	// Cascade: compact level+1 if it now exceeds its size budget.
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
	if err := f.Sync(); err != nil {
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
