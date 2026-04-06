package lmstree

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
