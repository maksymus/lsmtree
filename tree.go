package lmstree

// LSMTree is a simple implementation of a Log-Structured Merge Tree (LSM Tree).
// It uses an in-memory table (memTable) and a list of SSTables (sorted string tables).
// The memTable is flushed to disk when it reaches a certain size, and the SSTables are merged periodically.
// 			 |----------------------------------------------|
// Memory    |            		MemTable         			|
//           |----------------------------------------------|
//           |   W  |   (Level 1) SStables 				    |
// Disk      |   A  |   (Level 2) SStables    				|
//           |   L  |   (Level 3) SStables  			    |
// 		 	 |----------------------------------------------|

func NewLSMTree() *LSMTree {
	return &LSMTree{
		memTable: make(map[string]string),
		sstables: []map[string]string{},
	}
}

func (t *LSMTree) Put(key string, value any) {

}

func (t *LSMTree) Get(key string) any {
	return nil
}
