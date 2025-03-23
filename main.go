package main

import (
	"bytes"
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

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys()) // use the offset value of the last key
}

func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	for i := uint16(0); i < n; i++ {
		dst, src := dstNew+i, srcOld+i
		nodeAppendKV(new, dst, 0, old.getKey(src), old.getVal(src))
	}
}

func leafInsert(
	new BNode, old BNode, idx uint16, key []byte, val []byte,
) {
	new.setHeader(BNODE_LEAF, old.nbytes()+1)
	nodeAppendRange(new, old, 0, 0, idx)                    // copy the old keys from 0 to idx-1
	nodeAppendKV(new, idx, 0, key, val)                     // the new key
	nodeAppendRange(new, old, idx+1, idx, old.nbytes()-idx) // old keys from idx to old.nbytes()
}

func leafupdate(
	new BNode, old BNode, idx uint16, key []byte, val []byte,
) {
	new.setHeader(BNODE_LEAF, old.nbytes())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nbytes()-idx-1)
}

// find the last position that is less than or equal to the key
// TODO what if all positions are greater than the key and idx is -1?
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	var i uint16
	for i = 0; i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp == 0 {
			return i
		}
		if cmp > 0 {
			return i - 1
		}
	}
	return i - 1
}

func main() {
	nkeys := uint16(3)
	old := BNode(make([]byte, BTREE_PAGE_SIZE))
	old.setHeader(BNODE_LEAF, nkeys)
	nodeAppendKV(old, 0, 0, []byte("k1"), []byte("hi"))
	nodeAppendKV(old, 1, 0, []byte("k2"), []byte("a"))
	nodeAppendKV(old, 2, 0, []byte("k3"), []byte("hello"))
	printNode(old)

	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	new.setHeader(BNODE_LEAF, nkeys)
	nodeAppendKV(new, 0, 0, old.getKey(0), old.getVal(0))
	nodeAppendKV(new, 1, 0, []byte("k2"), []byte("b"))
	nodeAppendKV(new, 2, 0, old.getKey(2), old.getVal(2))
	old = new
	printNode(old)

	new = BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	new.setHeader(BNODE_LEAF, 4)
	nodeAppendKV(new, 0, 0, []byte("a"), []byte("b"))
	nodeAppendKV(new, 1, 0, old.getKey(0), old.getVal(0))
	nodeAppendKV(new, 2, 0, old.getKey(1), old.getVal(1))
	nodeAppendKV(new, 3, 0, old.getKey(2), old.getVal(2))
	old = new
	printNode(old)
}

func printNode(node BNode) {
	fmt.Printf("node has %v keys\n", node.nkeys())
	for i := uint16(0); i < node.nkeys(); i++ {
		fmt.Printf("index:%v, key:%v, val:%v\n", i, string(node.getKey(i)), string(node.getVal(i)))
	}
}
