package bplustree

import (
	"bytes"
	"sort"
	"unsafe"
)

type Node struct {
	keys   [][]byte
	num    int
	degree int
}

func (n *Node) Insert(key []byte, data []byte) (bool, []byte, unsafe.Pointer, unsafe.Pointer) {
	return false, nil, nil, nil
}

func (node *Node) isFull() bool {
	return node.degree*2-1 == node.num
}

type BranchNode struct {
	IsLeaf bool // must int first fields
	Node
	siblings []unsafe.Pointer
	parent   *BranchNode
}

func newBranchNode(degree int, parent *BranchNode) *BranchNode {
	return &BranchNode{
		Node: Node{
			keys:   make([][]byte, degree*2-1),
			num:    0,
			degree: degree,
		},
		siblings: make([]unsafe.Pointer, degree*2),
		parent:   parent,
	}
}

type LeafNode struct {
	IsLeaf bool // must int first fields
	Node
	Value  [][]byte
	Prev   *LeafNode
	Next   *LeafNode
	Parent *BranchNode
}

func newLeafNode(degree int, parent *BranchNode) *LeafNode {
	lf := &LeafNode{
		IsLeaf: true,
		Node: Node{
			keys:   make([][]byte, degree*2-1),
			num:    0,
			degree: degree,
		},
		Value:  make([][]byte, degree*2-1),
		Parent: parent,
	}
	return lf
}

type BPlusTree struct {
	root   *Node
	degree int
}

func (branch *BranchNode) Insert(key []byte, data []byte) (bool, []byte,
	unsafe.Pointer, unsafe.Pointer) {

	idx := sort.Search(branch.num, func(i int) bool {
		return bytes.Compare(branch.keys[i], key) >= 0
	})

	if branch.isFull() {

		z := newBranchNode(branch.degree, branch.parent)
		copy(z.keys, branch.keys[branch.degree:branch.num])
		copy(z.siblings, branch.siblings[branch.degree:branch.num+1])
		z.num = branch.degree - 1

		splitKey := branch.keys[branch.degree-1]

		branch.keys = branch.keys[:branch.degree-1]
		branch.siblings = branch.siblings[:branch.degree]
		branch.num = branch.degree - 1

		if idx > branch.degree-1 {
			z.insertNonFull(key, data, idx-branch.degree)
		} else {
			branch.insertNonFull(key, data, idx)
		}

		return true, splitKey, unsafe.Pointer(branch), unsafe.Pointer(z)

	}
	branch.insertNonFull(key, data, idx)
	return false, nil, nil, nil
}

func (branch *BranchNode) insertNonFull(key []byte, data []byte, idx int) {

	isLeaf := *(*bool)(branch.siblings[idx])

	var (
		split       bool
		splitKey    []byte
		left, right unsafe.Pointer
	)

	if !isLeaf {
		sibling := (*BranchNode)(branch.siblings[idx])
		split, splitKey, left, right = sibling.Insert(key, data)
	} else {
		sibling := (*LeafNode)(branch.siblings[idx])
		split, splitKey, left, right = sibling.Insert(key, data)
	}

	if !split {
		return
	}

	copy(branch.keys[idx+1:], branch.keys[idx:branch.num])
	copy(branch.siblings[idx+2:], branch.siblings[idx+1:branch.num+1])
	branch.keys[idx] = splitKey
	branch.siblings[idx] = left
	branch.siblings[idx+1] = right
	branch.num++
}

func (leafNode *LeafNode) Insert(key []byte, data []byte) (bool, []byte,
	unsafe.Pointer, unsafe.Pointer) {

	idx := sort.Search(leafNode.num, func(i int) bool {
		return bytes.Compare(leafNode.keys[i], key) >= 0
	})

	if bytes.Compare(leafNode.keys[idx], key) == 0 {
		leafNode.Value[idx] = append([]byte(nil), data...)
		return false, nil, nil, nil
	}

	if leafNode.isFull() {

		z := newLeafNode(leafNode.degree, leafNode.Parent)
		copy(z.keys, leafNode.keys[leafNode.degree-1:leafNode.num])
		z.num = leafNode.degree
		splitKey := z.keys[0]
		z.Prev = leafNode
		if leafNode.Next != nil {
			z.Next = leafNode.Next
			leafNode.Next.Prev = z
			leafNode.Next = z
		}

		leafNode.num = leafNode.degree - 1
		leafNode.keys = leafNode.keys[0 : leafNode.degree-1]

		if idx >= leafNode.degree-1 {
			z.insertNonFull(key, data, idx-leafNode.degree+1)
		} else {
			z.insertNonFull(key, data, idx)
		}
		return true, splitKey, unsafe.Pointer(leafNode), unsafe.Pointer(z)

	}

	leafNode.insertNonFull(key, data, idx)
	return false, nil, nil, nil
}

func (leafNode *LeafNode) insertNonFull(key []byte, data []byte, idx int) {

	copy(leafNode.keys[idx+1:leafNode.num+1], leafNode.keys[idx:leafNode.num])
	leafNode.keys[idx] = append([]byte(nil), key...)
	leafNode.Value[idx] = append([]byte(nil), data...)
	leafNode.num++
}
