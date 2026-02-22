package wal

import "github.com/maksymus/lmstree/util"

// NoopWAL is a write-ahead log implementation that does nothing.
// Useful for testing or when durability is not required.
type NoopWAL struct{}

func (n *NoopWAL) Write(entries ...*util.Entry) error {
	return nil
}

func (n *NoopWAL) CompareVersion(version string) int {
	return 1 // always ahead, so Recover() skips all files
}
