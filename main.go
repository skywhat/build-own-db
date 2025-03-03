package main

import (
	"fmt"
	"os"

	"math/rand"
)

func SaveData1(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = fp.Write(data)
	if err != nil {
		return err
	}
	return fp.Sync()
}

func SaveData2(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, rand.Intn(100000))
	fp, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()

	_, err = fp.Write(data) // write data to tmp file
	if err != nil {
		return err
	}
	if err := fp.Sync(); err != nil { // sync data to disk
		return err
	}
	return os.Rename(tmp, path) // replace the original file
}

type BNode struct {
	data []byte
}

const (
	BNODE_NODE = 1 // internal node without values
	BNODE_LEAF = 2 // leaf node with values
)

type BTree struct {
	root uint64
	// callbacks for managing on disk pages
	get func(uint64) BNode // dereference a pointer
	new func(BNode) uint64 // allocate a new page
	del func(uint64)       // deallocate a page
}

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func assert(cond bool) {
	if !cond {
		panic("assertion failed")
	}
}

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert(node1max <= BTREE_PAGE_SIZE)
}

func main() {
	SaveData2("test.txt", []byte("hello"))

}
