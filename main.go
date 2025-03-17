package main

import (
	"encoding/binary"
	"fmt"

	"github.com/build-own-db/util"
)

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

// read and write the child pointers array
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

func (node BNode) kvPos(idx uint16) uint16 {
	util.Assert(idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	util.Assert(idx <= node.nkeys())
	keyLenPos := node.kvPos(idx)
	keyLen := binary.LittleEndian.Uint16(node[keyLenPos:])
	return node[keyLenPos+4:][:keyLen]
}

func (node BNode) getVal(idx uint16) []byte {
	util.Assert(idx <= node.nkeys())
	keyLenPos := node.kvPos(idx)
	keyLen := binary.LittleEndian.Uint16(node[keyLenPos:])
	valLen := binary.LittleEndian.Uint16(node[keyLenPos+2:])
	return node[keyLenPos+4+keyLen:][:valLen]
}

func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos := new.kvPos(idx) // uses the offset value of the previous key
	// 4-byte KV sizes
	binary.LittleEndian.PutUint16(new[pos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key)+len(val)))
}

func main() {
	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	new.setHeader(BNODE_LEAF, 2)
	nodeAppendKV(new, 0, 0, []byte("k1"), []byte("hi"))
	nodeAppendKV(new, 1, 0, []byte("k3"), []byte("hello"))

	for i := uint16(0); i < 2; i++ {
		fmt.Printf("index:%v, key:%v, val:%v\n", i, string(new.getKey(i)), string(new.getVal(i)))
	}
}
