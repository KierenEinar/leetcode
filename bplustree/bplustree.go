package bplustree

import (
	"bytes"
	"container/list"
	"fmt"
	"sort"
	"unsafe"
)

type node struct {
	isLeaf   bool
	num      int
	degree   int
	keys     [][]byte
	parent   *node
	siblings []*node
	inode
}

type inode struct {
	values [][]byte
	prev   *node
	next   *node
}

func (node *node) isFull() bool {
	return node.degree*2-1 == node.num
}

func (node *node) isEnough() bool {
	return node.degree-1 <= node.num
}

func (node *node) canBorrow() bool {
	return node.degree == node.num
}

func newNode(degree int, parent *node, isLeaf bool) *node {
	node := &node{
		keys:     make([][]byte, degree*2-1),
		num:      0,
		isLeaf:   isLeaf,
		degree:   degree,
		siblings: make([]*node, degree*2),
		parent:   parent,
	}

	if isLeaf {
		node.values = make([][]byte, degree*2-1)
	}
	return node
}

type BPlusTree struct {
	root   *node
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
	root := tree.root
	root.Pretty()
}

func (n *node) Pretty() {

	levelFirstPtrM := make(map[unsafe.Pointer]struct{})

	// dfs first sibling
	path := n
	for {
		ptr := path.siblings[0]
		levelFirstPtrM[unsafe.Pointer(ptr)] = struct{}{}
		if ptr.isLeaf {
			break
		}
		path = ptr
	}

	queue := list.New()
	queue.PushBack(n)
	level := 0
	for queue.Len() > 0 {
		ele := queue.Front()
		queue.Remove(ele)
		nd := ele.Value.(*node)
		ptr := unsafe.Pointer(nd)
		if _, ok := levelFirstPtrM[ptr]; ok {
			level++
		}

		if !nd.isLeaf {
			fmt.Printf("level=%d, keys=%v\n", level, nd.keys[:nd.num])
			for idx := range nd.siblings[:nd.num+1] {
				queue.PushBack(nd.siblings[idx])
			}
		} else {
			fmt.Printf("leaf level=%d, keys=%v, values=%v, parent=%v\n", level,
				nd.keys[:nd.num], nd.values[:nd.num],
				nd.parent.keys[:nd.parent.num])
		}
	}
}

func (tree *BPlusTree) Insert(key []byte, data []byte) {

	if tree.root == nil {
		root := newNode(tree.degree, nil, true)
		insertLeafNonFull(root, key, data, 0)
		tree.root = root
		return
	}
	root := tree.root

	var (
		split       bool
		returnKey   []byte
		left, right *node
	)

	if root.isLeaf {
		split, returnKey, left, right = insertLeaf(root, key, data)
	} else {
		split, returnKey, left, right = insertBranch(root, key, data)
	}

	if !split {
		return
	}

	branchNode := newNode(tree.degree, nil, false)
	left.parent = branchNode
	right.parent = branchNode
	branchNode.siblings[0] = left
	branchNode.siblings[1] = right
	branchNode.keys[0] = returnKey
	branchNode.num = 1
	tree.root = branchNode

}

func (tree *BPlusTree) Get(key []byte) []byte {
	if tree.root == nil {
		return nil
	}
	root := tree.root
	if root.isLeaf {
		return leafGet(root, key)
	}
	return branchGet(root, key)
}

func branchGet(branch *node, key []byte) []byte {

	idx := sort.Search(branch.num, func(i int) bool {
		return bytes.Compare(branch.keys[i], key) >= 0
	})

	if idx != branch.degree*2-1 && bytes.Compare(branch.keys[idx], key) == 0 {
		idx++
	}

	sibling := branch.siblings[idx]
	if sibling.isLeaf {
		return leafGet(sibling, key)
	}
	return branchGet(sibling, key)
}

func leafGet(leaf *node, key []byte) []byte {

	idx := sort.Search(leaf.num, func(i int) bool {
		return bytes.Compare(leaf.keys[i], key) >= 0
	})

	if idx == leaf.num {
		return nil
	}

	if bytes.Compare(leaf.keys[idx], key) == 0 {
		return leaf.values[idx]
	}

	return nil
}

func insertBranch(branch *node, key []byte, data []byte) (bool, []byte,
	*node, *node) {

	idx := sort.Search(branch.num, func(i int) bool {
		return bytes.Compare(branch.keys[i], key) >= 0
	})

	if idx != branch.num && bytes.Compare(branch.keys[idx], key) == 0 {
		idx++
	}

	if branch.isFull() {

		z := branch.split()

		splitKey := branch.keys[branch.degree-1]
		branch.num = branch.degree - 1

		if idx > branch.degree-1 {
			insertBranchNonFull(z, key, data, idx-branch.degree)
		} else {
			insertBranchNonFull(branch, key, data, idx)
		}

		return true, splitKey, branch, z

	}
	insertBranchNonFull(branch, key, data, idx)
	return false, nil, nil, nil
}

func (branch *node) split() *node {
	z := newNode(branch.degree, branch.parent, branch.isLeaf)
	copy(z.keys, branch.keys[branch.degree:branch.num])
	copy(z.siblings, branch.siblings[branch.degree:branch.num+1])
	z.num = branch.degree - 1

	for idx := range z.siblings[:z.num+1] {
		sibling := z.siblings[idx]
		sibling.parent = z
	}
	return z
}

func insertBranchNonFull(n *node, key []byte, data []byte, idx int) {

	var (
		isLeaf   bool
		split    bool
		splitKey []byte
		left     *node
		right    *node
	)

	sibling := n.siblings[idx]
	isLeaf = sibling.isLeaf

	if !isLeaf {
		split, splitKey, left, right = insertBranch(sibling, key, data)
	} else {
		split, splitKey, left, right = insertLeaf(sibling, key, data)
	}
	if !split {
		return
	}
	copy(n.keys[idx+1:], n.keys[idx:n.num])
	copy(n.siblings[idx+2:], n.siblings[idx+1:n.num+1])
	n.keys[idx] = splitKey
	n.siblings[idx] = left
	n.siblings[idx+1] = right
	left.parent = n // 下面这两行意义不大, 可加可不加
	right.parent = n
	n.num++
}

func insertLeaf(n *node, key []byte, data []byte) (bool, []byte,
	*node, *node) {

	idx := sort.Search(n.num, func(i int) bool {
		return bytes.Compare(n.keys[i], key) >= 0
	})

	if idx < n.num && bytes.Compare(n.keys[idx], key) == 0 {
		n.values[idx] = append([]byte(nil), data...)
		return false, nil, nil, nil
	}

	if n.isFull() {
		z := newNode(n.degree, n.parent, true)
		copy(z.keys, n.keys[n.degree-1:n.num])
		copy(z.values, n.values[n.degree-1:n.num])
		z.num = n.degree
		splitKey := z.keys[0]
		z.prev = n
		if n.next != nil {
			z.next = n.next
			n.next.prev = z
			n.next = z
		}

		n.num = n.degree - 1

		if idx >= n.degree-1 {
			insertLeafNonFull(z, key, data, idx-n.degree+1)
		} else {
			insertLeafNonFull(n, key, data, idx)
		}
		return true, splitKey, n, z

	}

	insertLeafNonFull(n, key, data, idx)
	return false, nil, nil, nil
}

func insertLeafNonFull(leaf *node, key []byte, data []byte, idx int) {

	copy(leaf.keys[idx+1:leaf.num+1], leaf.keys[idx:leaf.num])
	copy(leaf.values[idx+1:leaf.num+1], leaf.values[idx:leaf.num])
	leaf.keys[idx] = append([]byte(nil), key...)
	leaf.values[idx] = append([]byte(nil), data...)
	leaf.num++
}

func (tree *BPlusTree) Remove(key []byte) bool {
	if tree.root == nil {
		return false
	}
	root := tree.root
	isLeaf := root.isLeaf

	var (
		flag bool
	)

	if isLeaf {
		flag = leafRemove(root, key)
	} else {
		branchRemove(root, key)
	}

	if !flag {
		return false
	}
	if root.num == 0 && root.siblings[0] != nil {
		tree.root = root.siblings[0]
	}
	return true
}

func branchRemove(branch *node, key []byte) bool {
	idx := sort.Search(branch.num, func(i int) bool {
		return bytes.Compare(branch.keys[i], key) >= 0
	})
	if idx != branch.degree*2-1 && bytes.Compare(branch.keys[idx], key) == 0 {
		idx++
	}
	var (
		flag bool
	)
	sibling := branch.siblings[idx]
	if !sibling.isLeaf {
		flag = branchRemove(sibling, key)
		if !sibling.isEnough() {
			reBalanceBranch(sibling, branch, idx)
		}
	} else {
		flag = leafRemove(sibling, key)
		if !sibling.isEnough() {
			reBalanceLeaf(sibling, branch, idx)
		}
	}
	return flag
}

func reBalanceBranch(branch *node, parent *node, idx int) {

	var (
		prev *node
		next *node
	)

	if parent == nil { // root
		return
	}

	if idx != 0 {
		prev = parent.siblings[idx-1]
	}
	if idx != parent.num {
		next = parent.siblings[idx+1]
	}

	if prev != nil && prev.canBorrow() {
		borrowPrevBranch(prev, branch, parent, idx)
	} else if next != nil && next.canBorrow() {
		borrowNextBranch(branch, next, parent, idx)
	} else {
		if prev != nil {
			mergeBranch(prev, branch, parent, idx-1)
		} else {
			mergeBranch(branch, next, parent, idx)
		}
	}
}

func mergeBranch(left *node, right *node, parent *node, parentIdx int) {

	left.keys[left.num] = parent.keys[parentIdx]
	left.num += right.num + 1

	copy(left.keys[left.num+1:left.num+1+right.num], right.keys[:right.num])
	copy(left.siblings[left.num+1:left.num+right.num+2], right.siblings[:right.num+1])

	copy(parent.keys[parentIdx:parent.num-1], parent.keys[parentIdx+1:parent.num])
	copy(parent.siblings[parentIdx+1:parent.num], parent.siblings[parentIdx+2:parent.num+1])

	parent.keys[parent.num-1] = nil
	parent.siblings[parent.num] = nil

	parent.num--
}

func borrowPrevBranch(prev *node, branch *node, parent *node, idx int) {

	copy(branch.keys[1:branch.num+1], branch.keys[:branch.num])
	copy(branch.siblings[1:branch.num+2], branch.siblings[:branch.num+1])
	branch.keys[0] = parent.keys[idx-1]
	branch.siblings[0] = prev.siblings[prev.num]
	branch.num++

	parent.keys[idx-1] = prev.keys[prev.num-1]
	prev.keys[prev.num-1] = nil
	prev.siblings[prev.num] = nil
	prev.num--
}

func borrowNextBranch(branch *node, next *node, parent *node, idx int) {

	branch.keys[branch.num] = parent.keys[idx]
	branch.siblings[branch.num+1] = next.siblings[0]
	branch.num++

	parent.keys[idx] = next.keys[0]

	copy(next.keys[0:next.num-1], next.keys[1:next.num])
	copy(next.siblings[0:next.num], next.siblings[1:next.num+1])
	next.num--
}

func changeKey(node *node, parent *node, parentIdx int) {
	parent.keys[parentIdx] = node.keys[0]
}

func leafRemove(leafNode *node, key []byte) bool {

	idx := sort.Search(leafNode.num, func(i int) bool {
		return bytes.Compare(leafNode.keys[i], key) >= 0
	})

	if idx == leafNode.num {
		return false
	}

	if bytes.Compare(leafNode.keys[idx], key) != 0 {
		return false
	}

	copy(leafNode.keys[idx:leafNode.num-1], leafNode.keys[idx+1:leafNode.num])
	copy(leafNode.values[idx:leafNode.num-1], leafNode.values[idx+1:leafNode.num])

	leafNode.keys[leafNode.num-1] = nil
	leafNode.values[leafNode.num-1] = nil
	leafNode.num--

	return true
}

func borrowPrevLeaf(leafNode *node, parent *node, parentIdx int) {

	prev := leafNode.prev
	copy(leafNode.keys[1:leafNode.num+1], leafNode.keys[:leafNode.num])
	copy(leafNode.values[1:leafNode.num+1], leafNode.values[:leafNode.num])
	leafNode.keys[0] = prev.keys[prev.num-1]
	leafNode.values[0] = prev.values[prev.num-1]
	prev.keys[prev.num-1] = nil
	prev.values[prev.num-1] = nil
	leafNode.num++
	prev.num--
	changeKey(leafNode, parent, parentIdx-1)
}

func borrowNextLeaf(leafNode *node, parent *node, parentIdx int) {
	next := leafNode.next
	leafNode.keys[leafNode.num] = next.keys[0]
	leafNode.values[leafNode.num] = next.values[0]
	leafNode.num++
	copy(next.keys[0:next.num-1], next.keys[1:next.num])
	copy(next.values[0:next.num-1], next.values[1:next.num])
	next.keys[next.num-1] = nil
	next.values[next.num-1] = nil

	next.num--
	changeKey(next, parent, parentIdx)
}

func mergeLeaf(left *node, right *node, parent *node, rightNodeParentIdx int) {

	copy(left.keys[left.num:left.num+right.num], right.keys[:right.num])
	copy(left.values[left.num:left.num+right.num], right.values[:right.num])
	left.num += right.num

	copy(parent.keys[rightNodeParentIdx-1:parent.num-1], parent.keys[rightNodeParentIdx:parent.num])
	copy(parent.siblings[rightNodeParentIdx-1:parent.num], parent.siblings[rightNodeParentIdx:parent.num+1])
	parent.keys[parent.num-1] = nil
	parent.siblings[parent.num] = nil
	parent.num--

	if right.next != nil {
		right.next.prev = left
	}
	left.next = right.next
}

func reBalanceLeaf(leafNode *node, parent *node, parentIdx int) {

	if leafNode.parent == nil { // root node
		return
	}

	if leafNode.prev != nil && leafNode.prev.canBorrow() {
		borrowPrevLeaf(leafNode, parent, parentIdx)
	} else if leafNode.next != nil && leafNode.next.canBorrow() {
		borrowNextLeaf(leafNode, parent, parentIdx)
	} else {
		if leafNode.prev != nil {
			mergeLeaf(leafNode.prev, leafNode, parent, parentIdx)
		} else {
			mergeLeaf(leafNode, leafNode.next, parent, parentIdx+1)
		}
	}
}
