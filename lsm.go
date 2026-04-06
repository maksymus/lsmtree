package lmstree

import (
	"os"

	"github.com/maksymus/lmstree/internal/memtable"
	walPkg "github.com/maksymus/lmstree/internal/wal"
)

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

	if err := mem.Recover(); err != nil {
		return nil, err
	}

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

// Delete marks key as deleted. The tombstone shadows any older value in SSTables
// until the next compaction removes both.
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

// Get returns the value for key, or nil and false if the key does not exist or
// has been deleted.
func (t *LSMTree) Get(key []byte) ([]byte, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Active MemTable has the freshest data.
	if e, ok := t.memTable.GetEntry(key); ok {
		if e.Tombstone {
			return nil, false
		}
		return e.Value, true
	}

	// Immutable MemTable is older than active but newer than any SSTable.
	if t.immutable != nil {
		if e, ok := t.immutable.GetEntry(key); ok {
			if e.Tombstone {
				return nil, false
			}
			return e.Value, true
		}
	}

	// Search SSTables level by level (L0 first = newest data).
	for _, level := range t.levels {
		for _, sst := range level {
			e, ok := sst.reader.Search(key)
			if !ok {
				continue
			}
			if e.Tombstone {
				return nil, false
			}
			return e.Value, true
		}
	}

	return nil, false
}

// Close stops the background flush worker, flushes remaining data to disk,
// closes all SSTable readers, and releases the WAL.
func (t *LSMTree) Close() error {
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
