package bplustree

import (
	"bytes"
	"container/list"
	"fmt"
	"sort"
	"unsafe"
)

type Node struct {
	keys   [][]byte
	num    int
	degree int
}

func (node *Node) isFull() bool {
	return node.degree*2-1 == node.num
}

func (node *Node) isEnough() bool {
	return node.degree-1 == node.num
}

func (node *Node) canBorrow() bool {
	return node.degree == node.num
}

type BranchNode struct {
	isLeaf bool // must int first fields
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
	isLeaf bool // must int first fields
	Node
	values [][]byte
	prev   *LeafNode
	next   *LeafNode
	parent *BranchNode
}

func newLeafNode(degree int, parent *BranchNode) *LeafNode {
	lf := &LeafNode{
		isLeaf: true,
		Node: Node{
			keys:   make([][]byte, degree*2-1),
			num:    0,
			degree: degree,
		},
		values: make([][]byte, degree*2-1),
		parent: parent,
	}
	return lf
}

type BPlusTree struct {
	root   unsafe.Pointer
	degree int
}

func NewPlusTree(degree int) *BPlusTree {
	return &BPlusTree{
		degree: degree,
	}
}

func (tree *BPlusTree) BFS() {
	if tree.root == nil {
		return
	}

	isLeaf := (*bool)(tree.root)
	if *isLeaf {
		leaf := (*LeafNode)(tree.root)
		k, v := leaf.Pretty()
		fmt.Printf("level 0, keys=%v, values=%v\n", k, v)
		return
	}

	branch := (*BranchNode)(tree.root)
	branch.Pretty()
}

func (branch *BranchNode) Pretty() {

	queue := list.New()
	queue.PushBack(unsafe.Pointer(branch))
	level := 0
	for queue.Len() > 0 {
		ele := queue.Front()
		queue.Remove(ele)
		ptr := ele.Value.(unsafe.Pointer)
		isLeaf := *(*bool)(ptr)

		if !isLeaf {
			branchNode := *(*BranchNode)(ptr)
			if branchNode.parent != nil && branchNode.parent.siblings[0] == ptr {
				level++
			}
		} else {
			leafNode := *(*LeafNode)(ptr)
			if leafNode.parent != nil && leafNode.parent.siblings[0] == ptr {
				level++
			}
		}

		if !isLeaf {
			branchNode := *(*BranchNode)(ptr)
			fmt.Printf("level=%d, keys=%v\n", level, branchNode.keys)
			for idx := range branchNode.siblings[:branchNode.num+1] {
				queue.PushBack(branchNode.siblings[idx])
			}
		} else {
			leafNode := *(*LeafNode)(ptr)
			fmt.Printf("leaf level=%d, keys=%v, values=%v\n", level,
				leafNode.keys[:leafNode.num], leafNode.values[:leafNode.num])
		}
	}
}

func (leaf *LeafNode) Pretty() (keys [][]byte, values [][]byte) {
	keys = make([][]byte, len(leaf.keys))
	values = make([][]byte, len(leaf.keys))
	for idx := range leaf.keys {
		keys[idx] = leaf.keys[idx]
		values[idx] = leaf.values[idx]
	}
	return
}

func (tree *BPlusTree) Insert(key []byte, data []byte) {

	if tree.root == nil {
		root := newLeafNode(tree.degree, nil)
		root.insertNonFull(key, data, 0)
		tree.root = unsafe.Pointer(root)
		return
	}
	root := tree.root
	isLeaf := (*bool)(root)
	var (
		split     bool
		returnKey []byte
		left      unsafe.Pointer
		right     unsafe.Pointer
	)
	if *isLeaf {
		leaf := (*LeafNode)(root)
		split, returnKey, left, right = leaf.Insert(key, data)
	} else {
		branch := (*BranchNode)(root)
		split, returnKey, left, right = branch.Insert(key, data)
	}
	if !split {
		return
	}
	branchNode := newBranchNode(tree.degree, nil)
	if *isLeaf {
		leftNode := (*LeafNode)(left)
		rightNode := (*LeafNode)(right)
		leftNode.parent = branchNode
		rightNode.parent = branchNode
	} else {
		leftNode := (*BranchNode)(left)
		rightNode := (*BranchNode)(right)
		leftNode.parent = branchNode
		rightNode.parent = branchNode
	}
	branchNode.keys[0] = returnKey
	branchNode.num = 1
	branchNode.siblings[0] = left
	branchNode.siblings[1] = right
	tree.root = unsafe.Pointer(branchNode)
	return

}

func (tree *BPlusTree) Remove(key []byte) bool {
	if tree.root == nil {
		return false
	}
	root := tree.root
	isLeaf := (*bool)(root)
	if *isLeaf {
		leafNode := (*LeafNode)(root)
		return leafNode.Remove(key)
	}

	branchNode := (*BranchNode)(root)
	flag := branchNode.Remove(key)
	if !flag {
		return false
	}

	if branchNode.num == 0 && branchNode.siblings[0] != nil {
		tree.root = branchNode.siblings[0]
	}
	return true
}

func (tree *BPlusTree) Get(key []byte) []byte {

	if tree.root == nil {
		return nil
	}
	root := tree.root
	isLeaf := (*bool)(root)

	if *isLeaf {
		leafNode := (*LeafNode)(root)
		return leafNode.Get(key)
	}

	branchNode := (*BranchNode)(root)
	return branchNode.Get(key)
}

func (branch *BranchNode) Get(key []byte) []byte {

	idx := sort.Search(branch.num, func(i int) bool {
		return bytes.Compare(branch.keys[i], key) >= 0
	})

	if bytes.Compare(branch.keys[idx], key) == 0 {
		idx++
	}

	isLeaf := *(*bool)(branch.siblings[idx])
	if isLeaf {
		sibling := *(*BranchNode)(branch.siblings[idx])
		return sibling.Get(key)
	}

	sibling := *(*LeafNode)(branch.siblings[idx])
	return sibling.Get(key)

}

func (branch *BranchNode) Insert(key []byte, data []byte) (bool, []byte,
	unsafe.Pointer, unsafe.Pointer) {

	idx := sort.Search(branch.num, func(i int) bool {
		return bytes.Compare(branch.keys[i], key) >= 0
	})

	if idx != branch.num && bytes.Compare(branch.keys[idx], key) == 0 {
		idx++
	}

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

func (branch *BranchNode) Remove(key []byte) bool {
	idx := sort.Search(branch.num, func(i int) bool {
		return bytes.Compare(branch.keys[i], key) >= 0
	})
	if bytes.Compare(branch.keys[idx], key) == 0 {
		idx++
	}
	var (
		flag bool
	)
	isLeaf := *(*bool)(branch.siblings[idx])
	if !isLeaf {
		sibling := (*BranchNode)(branch.siblings[idx])
		flag = sibling.Remove(key)
		if !sibling.isEnough() {
			sibling.reBalance(idx)
		}
	} else {
		sibling := (*LeafNode)(branch.siblings[idx])
		flag = sibling.Remove(key)
		if !sibling.isEnough() {
			sibling.reBalance(idx)
		}
	}
	return flag
}

func (branch *BranchNode) reBalance(idx int) {

	var (
		prev *BranchNode
		next *BranchNode
	)

	parent := branch.parent

	if parent == nil { // root
		return
	}

	if idx != 0 {
		prev = (*BranchNode)(parent.siblings[idx-1])
	}
	if idx != parent.num {
		next = (*BranchNode)(parent.siblings[parent.num])
	}

	if prev != nil && prev.canBorrow() {
		branch.borrowPrevSiblings(prev, idx)
	} else if next != nil && next.canBorrow() {
		branch.borrowNextSiblings(next, idx)
	} else {
		if prev != nil {
			mergeBranch(prev, branch, idx-1)
		} else {
			mergeBranch(branch, next, idx)
		}
	}
}

func mergeBranch(left *BranchNode, right *BranchNode, parentIdx int) {

	parent := left.parent
	left.keys[left.num] = parent.keys[parentIdx]
	left.num += right.num + 1

	copy(left.keys[left.num+1:left.num], right.keys[:right.num])
	copy(left.siblings[left.num+1:left.num+1], right.siblings[:right.num+1])

	copy(parent.keys[parentIdx:parent.num-1], parent.keys[parentIdx+1:parent.num])
	copy(parent.siblings[parentIdx+1:parent.num], parent.siblings[parentIdx+2:parent.num+1])

	//parent.keys[parent.num-1] = nil
	//parent.siblings[parent.num] = nil

	parent.num--
}

func (branch *BranchNode) borrowPrevSiblings(prev *BranchNode, idx int) {

	copy(branch.keys[1:branch.num+1], branch.keys[:branch.num])
	copy(branch.siblings[1:branch.num+2], branch.siblings[:branch.num+1])
	branch.keys[0] = prev.parent.keys[idx-1]
	branch.siblings[0] = prev.siblings[prev.num]
	branch.num++

	branch.parent.keys[idx-1] = prev.keys[prev.num-1]
	//prev.keys[prev.num-1] = nil
	//prev.siblings[prev.num] = nil
	prev.num--
}

func (branch *BranchNode) borrowNextSiblings(next *BranchNode, idx int) {

	branch.keys[branch.num] = branch.parent.keys[idx]
	branch.siblings[branch.num+1] = next.siblings[0]
	branch.num++

	branch.parent.keys[idx] = next.keys[0]

	copy(next.keys[0:next.num-1], next.keys[1:next.num])
	copy(next.siblings[1:next.num], next.siblings[1:next.num+1])
	next.num--
}

func (parent *BranchNode) changeKey(node *LeafNode, parentIdx int) {
	parent.keys[parentIdx] = node.keys[0]
}

func (leafNode *LeafNode) Insert(key []byte, data []byte) (bool, []byte,
	unsafe.Pointer, unsafe.Pointer) {

	idx := sort.Search(leafNode.num, func(i int) bool {
		return bytes.Compare(leafNode.keys[i], key) >= 0
	})

	if idx < leafNode.num && bytes.Compare(leafNode.keys[idx], key) == 0 {
		leafNode.values[idx] = append([]byte(nil), data...)
		return false, nil, nil, nil
	}

	if leafNode.isFull() {

		z := newLeafNode(leafNode.degree, leafNode.parent)
		copy(z.keys, leafNode.keys[leafNode.degree-1:leafNode.num])
		copy(z.values, leafNode.values[leafNode.degree-1:leafNode.num])
		z.num = leafNode.degree
		splitKey := z.keys[0]
		z.prev = leafNode
		if leafNode.next != nil {
			z.next = leafNode.next
			leafNode.next.prev = z
			leafNode.next = z
		}

		leafNode.num = leafNode.degree - 1

		if idx >= leafNode.degree-1 {
			z.insertNonFull(key, data, idx-leafNode.degree+1)
		} else {
			leafNode.insertNonFull(key, data, idx)
		}
		return true, splitKey, unsafe.Pointer(leafNode), unsafe.Pointer(z)

	}

	leafNode.insertNonFull(key, data, idx)
	return false, nil, nil, nil
}

func (leafNode *LeafNode) insertNonFull(key []byte, data []byte, idx int) {

	copy(leafNode.keys[idx+1:leafNode.num+1], leafNode.keys[idx:leafNode.num])
	copy(leafNode.values[idx+1:leafNode.num+1], leafNode.values[idx:leafNode.num])
	leafNode.keys[idx] = append([]byte(nil), key...)
	leafNode.values[idx] = append([]byte(nil), data...)
	leafNode.num++
}

func (leafNode *LeafNode) Remove(key []byte) bool {

	idx := sort.Search(leafNode.num, func(i int) bool {
		return bytes.Compare(leafNode.keys[i], key) == 0
	})

	if idx == leafNode.num {
		return false
	}

	copy(leafNode.keys[idx:leafNode.num-1], leafNode.keys[idx+1:leafNode.num])
	copy(leafNode.values[idx:leafNode.num-1], leafNode.values[idx+1:leafNode.num])

	//leafNode.keys[leafNode.num] = nil
	//leafNode.values[leafNode.num] = nil
	leafNode.num--

	return true
}

func (leafNode *LeafNode) Get(key []byte) []byte {

	idx := sort.Search(leafNode.num, func(i int) bool {
		return bytes.Compare(leafNode.keys[i], key) == 0
	})

	if idx == leafNode.num {
		return nil
	}

	return leafNode.values[idx]
}

func (leafNode *LeafNode) borrowPrevSibling(parentIdx int) {

	prev := leafNode.prev
	copy(leafNode.keys[1:leafNode.num+1], leafNode.keys[:leafNode.num])
	copy(leafNode.values[1:leafNode.num+1], leafNode.values[:leafNode.num])
	leafNode.keys[0] = prev.keys[prev.num-1]
	leafNode.values[0] = prev.values[prev.num-1]
	//prev.keys[prev.num-1] = nil
	//prev.values[prev.num-1] = nil
	leafNode.num++
	prev.num--
	leafNode.parent.changeKey(leafNode, parentIdx-1)
}

func (leafNode *LeafNode) borrowNextSibling(parentIdx int) {
	next := leafNode.next
	leafNode.keys[leafNode.num] = next.keys[0]
	leafNode.values[leafNode.num] = next.values[0]
	leafNode.num++
	copy(next.keys[0:next.num-1], next.keys[1:next.num])
	copy(next.values[0:next.num-1], next.values[1:next.num])
	//next.keys[next.num-1] = nil
	//next.values[next.num-1] = nil

	next.num--
	next.parent.changeKey(next, parentIdx)
}

func mergeLeaf(left *LeafNode, right *LeafNode, parentIdx int) {

	copy(left.keys[left.num:left.num+right.num], right.keys[:right.num])
	copy(left.values[left.num:left.num+right.num], right.values[:right.num])
	left.num += right.num
	if left.parent != nil {
		copy(left.parent.keys[parentIdx-1:left.parent.num-1], left.parent.keys[parentIdx:left.parent.num])
		copy(left.parent.siblings[parentIdx-1:left.parent.num], left.parent.siblings[parentIdx:left.parent.num+1])
		//left.parent.keys[left.parent.num-1] = nil
		//left.parent.siblings[left.parent.num] = nil
		left.parent.num--
	}
}

func (leafNode *LeafNode) reBalance(parentIdx int) {

	if leafNode.parent == nil { // root node
		return
	}

	if leafNode.prev != nil && leafNode.prev.canBorrow() {
		leafNode.borrowPrevSibling(parentIdx)
	} else if leafNode.next != nil && leafNode.next.canBorrow() {
		leafNode.borrowNextSibling(parentIdx)
	} else {
		if leafNode.prev != nil {
			mergeLeaf(leafNode.prev, leafNode, parentIdx)
		} else {
			mergeLeaf(leafNode, leafNode.next, parentIdx+1)
		}
	}
}
