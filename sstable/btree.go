package sstable

import (
	"bytes"
	"sort"
)

type BTree struct {
	degree int
	root   *BTreeNode
}

type BTreeNode struct {
	isLeaf   bool
	keys     [][]byte
	values   [][]byte
	siblings []*BTreeNode
	num      int
	degree   int
}

func (node *BTreeNode) isFull() bool {
	return node.num*2-1 == node.degree
}

func newNode(degree int, isLeaf bool) *BTreeNode {
	node := &BTreeNode{
		isLeaf:   isLeaf,
		degree:   degree,
		keys:     make([][]byte, degree*2-1),
		values:   make([][]byte, degree*2-1),
		siblings: make([]*BTreeNode, degree*2),
		num:      0,
	}
	return node
}

func InitBTree(degree int) *BTree {
	return &BTree{
		degree: degree,
	}
}

func (btree *BTree) Insert(key, value []byte) {
	root := btree.root
	if root == nil {
		n := newNode(btree.degree, true)
		root = n
	}

	if root.isFull() {
		n := newNode(btree.degree, false)
		k, v := root.keys[btree.degree], root.values[btree.degree]
		z := root.splitChild()
		n.setKVAndSibling(0, k, v, root, z)
		root = n
	}
	insertNonFull(root, key, value)
	btree.root = root
}

// must assert idx less than node.num and node must not full
func (node *BTreeNode) setKVAndSibling(idx int, key, value []byte, left, right *BTreeNode) {
	assert(idx < node.num && !node.isFull())
	copy(node.keys[idx+1:node.num+1], node.keys[idx:node.num])
	copy(node.values[idx+1:node.num+1], node.values[idx:node.num])
	node.keys[idx] = key
	node.values[idx] = value

	copy(node.siblings[idx+2:node.num+2], node.siblings[idx+1:node.num+1])
	node.siblings[idx] = left
	node.siblings[idx+1] = right
	node.num++
}

// must assert node is full
func (node *BTreeNode) splitChild() *BTreeNode {
	assert(node.isFull())

	t := node.degree
	z := newNode(t, node.isLeaf)

	copy(z.keys, node.keys[t:node.num])
	copy(z.values, node.values[t:node.num])
	z.num = t - 1
	if !node.isLeaf {
		// t = 3
		// keys  	0   1    2   3    4
		// sib   0    1    2   3    4    5
		copy(z.siblings, node.siblings[t:node.num+1])
	}
	return z
}

func insertNonFull(node *BTreeNode, key, value []byte) {

	idx := sort.Search(node.num, func(i int) bool {
		return bytes.Compare(node.keys[i], key) >= 0
	})

	var found bool

	if idx < node.num {
		found = bytes.Compare(node.keys[idx], key) == 0
	}

	if found {
		node.values[idx] = value
		return
	}

	if node.isLeaf {
		copy(node.keys[idx+1:node.num+1], node.keys[idx:node.num])
		copy(node.values[idx+1:node.num+1], node.values[idx:node.num])
		node.num++
		node.keys[idx] = append([]byte(nil), key...)
		node.values[idx] = append([]byte(nil), value...)
		return
	}

	sibling := node.siblings[idx]

	if !sibling.isFull() {
		insertNonFull(node.siblings[idx], key, value)
		return
	}

	k, v := sibling.keys[idx], sibling.values[idx]

	z := sibling.splitChild()
	node.setKVAndSibling(idx, k, v, sibling, z)

	if bytes.Compare(k, key) > 0 {
		insertNonFull(z, key, value)
	} else {
		insertNonFull(sibling, key, value)
	}
}

func (btree *BTree) Remove(key []byte) bool {

}

// note, caller should follow this rules
// * only root node's num can lt degree if is root
// * other wise node's num should be gte than degree
func remove(node *BTreeNode, key []byte) bool {

	idx := sort.Search(node.num, func(i int) bool {
		return bytes.Compare(node.keys[i], key) >= 0
	})

	var found bool

	if idx < node.num {
		found = bytes.Compare(node.keys[idx], key) == 0
	}

	if node.isLeaf && !found {
		return false
	}

	if found {

		if node.isLeaf {
			copy(node.keys[idx:node.num-1], node.keys[idx+1:node.num])
			copy(node.values[idx:node.num-1], node.values[idx+1:node.num])
			node.num--
		} else {
			prevSibling := node.siblings[idx]
			nextSibling := node.siblings[idx+1]

			k, v := node.keys[idx], node.values[idx]

			// left sibling is enough
			if prevSibling.num > node.degree-1 {

				mostlyPrevious := prevSibling

				// search mostly previous key
				for !mostlyPrevious.isLeaf {
					mostlyPrevious = mostlyPrevious.siblings[mostlyPrevious.num]
				}

				moveKey := mostlyPrevious.keys[mostlyPrevious.num-1]
				moveValue := mostlyPrevious.values[mostlyPrevious.num-1]
				node.keys[idx] = moveKey
				node.values[idx] = moveValue

				mostlyPrevious.keys[mostlyPrevious.num-1] = k
				mostlyPrevious.values[mostlyPrevious.num-1] = v

				remove(prevSibling, key)

			}

		}

		return true
	}

}

func assert(condition bool, msg ...string) {
	if !condition {
		panic(msg)
	}
}
