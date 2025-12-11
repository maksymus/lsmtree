package lmstree

import (
	"fmt"
	"time"

	"github.com/maksymus/lmstree/wal"
)

func main() {

	createdAt := time.Now()
	version := fmt.Sprintf("%s-%d", createdAt.Format("20060102150405"), createdAt.Nanosecond())

	fmt.Println("LSM Tree Version:", version)
	fileName := "wal-" + version + ".log"
	v, e := wal.VersionFromFileName(fileName)
	fmt.Println("Extracted Version:", v, e)

	//lsmTree := NewLSMTree()
	//lsmTree.Insert("exampleKey", "exampleValue")
	//value := lsmTree.Get("exampleKey")
	//fmt.Println("Value for 'exampleKey':", value)
}

type LSMTree struct {
	memTable map[string]string
	sstables []map[string]string
}
