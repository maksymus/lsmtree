package main

import (
	"fmt"
	"time"

	"github.com/maksymus/lmstree/internal/wal"
)

func main() {
	createdAt := time.Now()
	version := fmt.Sprintf("%s-%d", createdAt.Format("20060102150405"), createdAt.Nanosecond())

	fmt.Println("LSM Tree Version:", version)
	fileName := "wal-" + version + ".log"
	v, e := wal.VersionFromFileName(fileName)
	fmt.Println("Extracted Version:", v, e)
}
