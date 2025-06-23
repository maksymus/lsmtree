package main

func NewLSMTree() *LSMTree {
	return &LSMTree{
		memTable: make(map[string]string),
		sstables: []map[string]string{},
	}
}

func (t *LSMTree) Insert(key string, value any) {

}

func (t *LSMTree) Get(key string) any {
	return nil
}
