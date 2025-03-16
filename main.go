package main

import (
	"encoding/binary"
	"fmt"
	"os"

	"math/rand"

	"github.com/build-own-db/util"
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

type BNode []byte // can be dumped to the disk

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

func init() {
	// type:2B + nkeys:2B + pointers:nkeys*8B + offsets:nkeys*8B + key-values(key_size:2B + val_size:2B + key + val)
	node1max := HEADER + 1*8 + 1*2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	util.Assert(node1max <= BTREE_PAGE_SIZE) // maximum kv
}

// header
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

func (node BNode) getPtr(idx uint16) uint64 {
	util.Assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	util.Assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

func offsetPos(node BNode, idx uint16) uint16 {
	util.Assert(idx >= 1 && idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}

func main() {
	SaveData2("test.txt", []byte("hello"))
}
