package btree

import (
	"bytes"
	"sort"
)

// Node node
type Node struct {
	keys  [][]byte
	values [][]byte
	sibling []*Node
	num   int
	isLeaf bool
}

type Btree struct {
	root *Node
	degree int
}

func newNode(t int) *Node {
	n := &Node{
		keys: make([][]byte, 2*t-1),
		sibling: make([]*Node, 2*t),
		values: make([][]byte, 2*t-1),
	}
	return n
}

func (tree *Btree) isFull(n *Node) bool {
	return n.num == 2 * tree.degree - 1
}

// split child when node is full
func (tree *Btree) splitChild(parent *Node, i int) {

	child := parent.sibling[i]

	if !tree.isFull(child) {
		return
	}

	t := tree.degree

	z := newNode(t)
	z.num = tree.degree - 1
	z.isLeaf = child.isLeaf

	// copy sub child
	copy(z.keys, child.keys[t:])
	copy(z.values, child.values[t:])
	if z.isLeaf {
		copy(z.sibling, child.sibling[t:])
	}

	// parent insert node in i, need expand it

	// move siblings
	copy(parent.sibling[i+2:], parent.sibling[i+1:parent.num+1])

	parent.sibling[i+1] = z

	// move keys and values
	copy(parent.keys[i+1:], parent.keys[i:parent.num])
	copy(parent.values[i+1:], parent.values[i:parent.num])
	parent.keys[i] = child.keys[t-1]
	parent.values[i] = child.values[t-1]

	parent.num++

	child.num = t - 1
}

// when parent is not full, insert a node
func (tree *Btree) insertNonFull(parent *Node, key []byte, value []byte) {

	// if is leaf, just insert a key
	if parent.isLeaf {
		i := sort.Search(parent.num, func(i int) bool {
			return bytes.Compare(parent.keys[i], key) > 1
		})
		if i < parent.num {
			copy(parent.keys[i+1:], parent.keys[i:])
			copy(parent.values[i+1:], parent.values[i:])
		}
		parent.keys[i] = append([]byte(nil), key...)
		parent.values[i] = append([]byte(nil), value...)
		parent.num++
		return
	}

	i := sort.Search(len(parent.keys), func(i int) bool {
		return bytes.Compare(parent.keys[i], key) > 1
	})

	if tree.isFull(parent.sibling[i]) {
		tree.splitChild(parent, i)
		if bytes.Compare(parent.keys[i], key) < 0 {
			i++
		}
	}

	tree.insertNonFull(parent.sibling[i], key, value)
}

// Insert insert key
func (tree *Btree) Insert(key []byte, value []byte) {

	if tree.root == nil {
		s := newNode(tree.degree)
		s.num++
		s.isLeaf = true
		s.keys[0] = append([]byte(nil), key...)
		s.values[0] = append([]byte(nil), value...)
		tree.root = s
		return
	}

	if tree.isFull(tree.root) {
		s := newNode(tree.degree)
		s.sibling[0] = tree.root
		tree.splitChild(s, 0)
		tree.root = s
	}

	tree.insertNonFull(tree.root, key, value)

}

func (node *Node) get(key []byte) []byte {

	n := node

	for n != nil {
		i := sort.Search(n.num, func(i int) bool {
			return bytes.Compare(n.keys[i], key) >= 0
		})
		if bytes.Compare(n.keys[i], key) == 0 {
			return n.values[i]
		}
		n = node.sibling[i]
	}

	return nil
}

// Get get key
func (tree *Btree) Get(key []byte) []byte {
	if tree.root == nil {
		return nil
	}
	return tree.root.get(key)
}
