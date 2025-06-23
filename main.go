package main

import "fmt"

func main() {
	lsmTree := NewLSMTree()
	lsmTree.Insert("exampleKey", "exampleValue")
	value := lsmTree.Get("exampleKey")
	fmt.Println("Value for 'exampleKey':", value)
}

type LSMTree struct {
	memTable map[string]string
	sstables []map[string]string
}
