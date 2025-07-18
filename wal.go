package main

import (
	"os"
	"sync"
)

// WAL represents a Write-Ahead Log (WAL) for the LSM tree.
// It is used to log changes before they are applied to the LSM tree.
// The WAL is stored in a file and is used to recover the state of the LSM tree in case of a crash.
type WAL struct {
	mutex   sync.Mutex
	file    os.File
	dir     string
	path    string
	version string
}
