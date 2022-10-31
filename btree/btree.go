package btree

import (
	"bytes"
	"sort"
)

// Node node
type Node struct {
	keys    [][]byte
	values  [][]byte
	sibling []*Node
	num     int
	isLeaf  bool
}

type Btree struct {
	root   *Node
	degree int
}

func newNode(t int, isLeaf bool) *Node {
	n := &Node{
		keys:    make([][]byte, 2*t-1),
		sibling: make([]*Node, 2*t),
		values:  make([][]byte, 2*t-1),
		isLeaf:  isLeaf,
	}
	return n
}

func (tree *Btree) isFull(n *Node) bool {
	return n.num == 2*tree.degree-1
}

// split child when node is full
func (tree *Btree) splitChild(parent *Node, i int) {

	child := parent.sibling[i]

	if !tree.isFull(child) {
		return
	}

	t := tree.degree

	z := newNode(t, child.isLeaf)
	z.num = t - 1

	// copy sub child
	copy(z.keys, child.keys[t:2*t-1])
	copy(z.values, child.values[t:2*t-1])
	if !z.isLeaf {
		copy(z.sibling, child.sibling[t:2*t])
	}

	// parent insert node in i, need expand it

	// move siblings
	copy(parent.sibling[i+1:], parent.sibling[i:parent.num+1])
	child.num = t - 1
	parent.sibling[i+1] = z

	// move keys and values
	copy(parent.keys[i+1:], parent.keys[i:parent.num])
	copy(parent.values[i+1:], parent.values[i:parent.num])
	parent.keys[i] = child.keys[t-1]
	parent.values[i] = child.values[t-1]

	parent.num++
}

// when parent is not full, insert a node
func (tree *Btree) insertNonFull(parent *Node, key []byte, value []byte) {

	i := sort.Search(parent.num, func(i int) bool {
		return bytes.Compare(parent.keys[i], key) > 1
	})

	// if is leaf, just insert a key
	if parent.isLeaf {
		if i < parent.num {
			copy(parent.keys[i+1:], parent.keys[i:])
			copy(parent.values[i+1:], parent.values[i:])
		}
		parent.keys[i] = append([]byte(nil), key...)
		parent.values[i] = append([]byte(nil), value...)
		parent.num++
		return
	}

	if tree.isFull(parent.sibling[i]) {
		tree.splitChild(parent, i)
		if bytes.Compare(parent.keys[i], key) < 0 {
			i++
		}
	}

	tree.insertNonFull(parent.sibling[i], key, value)
}

func (node *Node) get(key []byte) []byte {

	n := node

	for {
		i := sort.Search(n.num, func(i int) bool {
			return bytes.Compare(n.keys[i], key) >= 0
		})
		if i < n.num && bytes.Compare(n.keys[i], key) == 0 {
			return n.values[i]
		}

		if n.isLeaf {
			return nil
		}

		n = node.sibling[i]
	}

}

// Get get key
func (tree *Btree) Get(key []byte) []byte {
	if tree.root == nil {
		return nil
	}
	return tree.root.get(key)
}

func (tree *Btree) remove(node *Node, key []byte) bool {

	idx := sort.Search(node.num, func(i int) bool {
		return bytes.Compare(node.keys[i], key) >= 1
	})

	// found case
	if idx < node.num && bytes.Compare(node.keys[idx], key) == 0 {

		// if node is leaf, then remove it
		if node.isLeaf {
			copy(node.keys[idx:], node.keys[idx+1:])
			copy(node.values[idx:], node.values[idx+1:])
			node.num--
			return true
		} else {

			// just the internal node
			// case 1:
			// if left sibling keys num gte t, then switch the position between node and its previous node
			if node.sibling[idx].num > tree.degree-1 {

				previous := node.sibling[idx]
				for !previous.isLeaf {
					previous = previous.sibling[previous.num]
				}
				// switch pos
				previous.keys[previous.num-1], node.keys[idx] = node.keys[idx], previous.keys[previous.num-1]
				previous.values[previous.num-1], node.values[idx] = node.values[idx], previous.values[previous.num-1]
				tree.remove(node.sibling[idx], key)
			} else if node.sibling[idx+1].num > tree.degree-1 {
				// case 2:
				// this case is similar to the case 1
				next := node.sibling[idx+1]
				for !next.isLeaf {
					next = next.sibling[0]
				}

				next.keys[0], node.keys[idx] = node.keys[idx], next.keys[0]
				next.values[0], node.values[idx] = node.values[idx], next.values[0]
				tree.remove(node.sibling[idx+1], key)
			} else {
				// case 3:
				// in this case, left sibling and right sibling nums must be t-1(according to the btree privacy),
				// so merge [left, node, right] into a new node
				tree.merge(node, idx)
				tree.remove(node.sibling[idx], key)
			}

		}

	} else { // not found in this node, so need to sink sibling seek and remove

		if node.isLeaf {
			return false
		}

		// sibling need to steal or merge with neighbor sibling
		if node.sibling[idx].num == tree.degree-1 {

			// left sibling is enough to borrow
			if idx != 0 && node.sibling[idx-1].num > tree.degree-1 {

				cur := node.sibling[idx]
				pre := node.sibling[idx-1]

				// node insert in cur[0]
				// step 1: change cur node
				copy(cur.keys[1:cur.num+1], cur.keys[:cur.num])
				copy(cur.values[1:cur.num+1], cur.values[:cur.num])
				if !cur.isLeaf {
					copy(cur.sibling[1:], cur.sibling[:cur.num+1])
					cur.sibling[0] = pre.sibling[pre.num]
				}
				cur.keys[0] = pre.keys[pre.num-1]
				cur.values[0] = pre.values[pre.num-1]
				cur.num++

				// step 2: change node idx key
				node.keys[idx] = pre.keys[pre.num-1]
				node.values[idx] = pre.values[pre.num-1]

				// step 3: change pre node
				pre.keys[pre.num-1] = nil   // help gc
				pre.values[pre.num-1] = nil // help gc
				pre.sibling[pre.num] = nil  // help gc

				pre.num--

				tree.remove(cur, key)

			} else if idx != node.num && node.sibling[idx].num > tree.degree-1 { // right sibling is enough to borrow

				cur := node.sibling[idx]
				next := node.sibling[idx+1]

				// step 1
				// change cur sibling (if have) and keys
				cur.keys[cur.num] = node.keys[idx]
				if !cur.isLeaf {
					cur.sibling[cur.num+1] = next.sibling[0]
				}
				cur.num++

				// step 2
				// change node
				node.keys[idx] = next.keys[0]

				// step 3
				// change next sibling
				copy(next.keys[:next.num-1], next.keys[1:next.num])
				if !next.isLeaf {
					copy(next.sibling[:next.num], next.sibling[1:next.num+1])
					next.sibling[next.num+1] = nil // help gc
				}
				next.num--
				tree.remove(cur, key)
			} else {

				// merge right sibling if have
				if idx < node.num {
					tree.merge(node, idx)
					tree.remove(node.sibling[idx], key)
				} else { // merge left sibling
					tree.merge(node, idx-1)
					tree.remove(node.sibling[idx-1], key)
				}

			}

		} else {
			tree.remove(node.sibling[idx], key)
		}
	}

	return false
}

func (tree *Btree) merge(node *Node, keyi int) {
	prevSibling := node.sibling[keyi]
	nextSibling := node.sibling[keyi+1]

	// node idx key will be move to siblings, so sibling [idx+1:num+1] will move forward [idx:num+1],
	// and key will do the same
	copy(node.sibling[keyi+1:node.num+1], node.sibling[keyi+2:node.num+1])
	copy(node.keys[keyi:node.num], node.keys[keyi+1:node.num])
	node.num--

	prevSibling.keys[prevSibling.num] = node.keys[keyi]
	copy(prevSibling.keys[prevSibling.num+1:], nextSibling.keys[:nextSibling.num])
	if prevSibling.isLeaf {
		copy(prevSibling.sibling[prevSibling.num+1:], nextSibling.sibling[:nextSibling.num+1])
	}

	prevSibling.num = tree.degree*2 - 1

	nextSibling = nil // help gc
}

// NewTree new tree
func NewTree(degree int) *Btree {
	return &Btree{
		degree: degree,
	}
}

// Insert insert key
func (tree *Btree) Insert(key []byte, value []byte) {

	if tree.root == nil {
		s := newNode(tree.degree, true)
		s.num++
		s.keys[0] = append([]byte(nil), key...)
		s.values[0] = append([]byte(nil), value...)
		tree.root = s
		return
	}

	n := tree.root

	if tree.isFull(tree.root) {
		s := newNode(tree.degree, false)
		s.sibling[0] = tree.root
		tree.splitChild(s, 0)
		tree.root = s
		if bytes.Compare(s.keys[0], key) < 0 {
			n = s.sibling[1]
		} else {
			n = s.sibling[0]
		}
	}

	tree.insertNonFull(n, key, value)

}

// Remove the key
func (tree *Btree) Remove(key []byte) bool {

	if tree.root == nil {
		return false
	}

	r := tree.remove(tree.root, key)

	if tree.root.num == 0 {
		if tree.root.isLeaf { // case 1 if root node is leaf and empty
			tree.root = nil
		} else { // case 2 root merge with left and right sibling
			tree.root = tree.root.sibling[0]
		}
	}
	return r

}
