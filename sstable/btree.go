package sstable

import (
	"bytes"
	"fmt"
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
	return node.num == node.degree*2-1
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
		k, v := root.keys[btree.degree-1], root.values[btree.degree-1]
		z := root.splitChild()
		n.setKVAndSibling(0, k, v, root, z)
		root = n
	}
	insertNonFull(root, key, value)
	btree.root = root
}

// must assert idx less than node.num and node must not full
func (node *BTreeNode) setKVAndSibling(idx int, key, value []byte, left, right *BTreeNode) {
	assert(!node.isFull())
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
	if !node.isLeaf {
		// t = 3
		// keys  	0   1    2   3    4
		// sib   0    1    2   3    4    5
		copy(z.siblings, node.siblings[t:node.num+1])
	}
	z.num = t - 1
	node.num = t - 1
	for idx := node.num; idx < len(node.keys); idx++ {
		node.keys[idx] = nil
		node.values[idx] = nil
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

	k, v := sibling.keys[sibling.degree-1], sibling.values[sibling.degree-1]

	z := sibling.splitChild()
	node.setKVAndSibling(idx, k, v, sibling, z)

	if bytes.Compare(k, key) < 0 {
		insertNonFull(z, key, value)
	} else {
		insertNonFull(sibling, key, value)
	}
}

func (btree *BTree) Remove(key []byte) bool {

	root := btree.root
	if root == nil {
		return false
	}

	r := remove(root, key)
	if root.num == 0 {
		if root.isLeaf {
			btree.root = nil
		} else {
			btree.root = root.siblings[0]
		}
	}
	return r
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
			node.keys[node.num-1] = nil
			node.values[node.num-1] = nil
			node.num--
			return true
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

				return remove(prevSibling, key)

			} else if nextSibling.num > node.degree-1 {

				mostLatest := nextSibling
				for !mostLatest.isLeaf {
					mostLatest = mostLatest.siblings[0]
				}

				moveKey := mostLatest.keys[0]
				moveValue := mostLatest.values[0]
				node.keys[idx] = moveKey
				node.values[idx] = moveValue

				mostLatest.keys[0] = k
				mostLatest.values[0] = v

				return remove(nextSibling, key)

			} else { // merge
				merge(node, idx)
				return remove(node.siblings[idx], key)
			}

		}
	} else {

		sibling := node.siblings[idx]

		if sibling.num == node.degree-1 {

			var (
				prev *BTreeNode
				next *BTreeNode
			)

			if idx != node.num {
				next = node.siblings[idx+1]
			}

			if idx != 0 {
				prev = node.siblings[idx-1]
			}

			if prev != nil && prev.num > prev.degree-1 {

				nodeKey := node.keys[idx-1]
				nodeVal := node.values[idx-1]

				// sibling borrow prev
				copy(sibling.keys[1:], sibling.keys[:node.num])
				copy(sibling.values[1:], sibling.values[:node.num])

				node.keys[idx-1] = prev.keys[prev.num-1]
				node.values[idx-1] = prev.values[prev.num-1]

				sibling.keys[0] = nodeKey
				sibling.values[0] = nodeVal

				prev.keys[prev.num-1] = nil
				prev.values[prev.num-1] = nil

				if !sibling.isLeaf {
					copy(sibling.siblings[1:], sibling.siblings[:node.num+1])
					sibling.siblings[0] = prev.siblings[prev.num]
					prev.siblings[prev.num] = nil
				}

				sibling.num++
				prev.num--

				return remove(sibling, key)

			} else if next != nil && next.num > next.degree-1 {
				// sibling borrow next

				nodeKey := node.keys[idx]
				nodeVal := node.values[idx]

				sibling.keys[sibling.num] = nodeKey
				sibling.values[sibling.num] = nodeVal

				node.keys[idx] = next.keys[0]
				node.values[idx] = next.values[0]

				copy(next.keys[0:], next.keys[1:next.num])
				copy(next.values[0:], next.values[1:next.num])

				next.keys[next.num-1] = nil
				next.values[next.num-1] = nil

				if sibling.isLeaf {
					sibling.siblings[sibling.num+1] = next.siblings[0]
					copy(next.siblings[0:], next.siblings[1:next.num+1])
					next.siblings[next.num] = nil
				}
				sibling.num++
				next.num--

				return remove(sibling, key)

			} else {

				if prev != nil {
					// merge prev
					merge(node, idx-1)
					return remove(node.siblings[idx-1], key)
				} else {
					// merge next
					merge(node, idx)
					return remove(node.siblings[idx], key)
				}
			}

		} else {
			return remove(sibling, key)
		}

	}
}

func merge(node *BTreeNode, idx int) {
	prevSibling := node.siblings[idx]
	nextSibling := node.siblings[idx+1]

	copy(prevSibling.keys[prevSibling.num+1:], nextSibling.keys[:nextSibling.num])
	copy(prevSibling.values[prevSibling.num+1:], nextSibling.values[:nextSibling.num])

	prevSibling.keys[prevSibling.num] = node.keys[idx]
	prevSibling.values[prevSibling.num] = node.values[idx]

	if !prevSibling.isLeaf {
		copy(prevSibling.siblings[prevSibling.num+1:], nextSibling.siblings[:nextSibling.num+1])
	}

	copy(node.keys[idx:], node.keys[idx+1:node.num])
	copy(node.values[idx:], node.values[idx+1:node.num])
	copy(node.siblings[idx+1:], node.siblings[idx+2:node.num+1])

	node.keys[node.num-1] = nil
	node.values[node.num-1] = nil
	node.siblings[node.num] = nil

	prevSibling.num += nextSibling.num + 1
	node.num -= 1
	nextSibling = nil
}

func (btree *BTree) Get(key []byte) ([]byte, bool) {
	if btree.root == nil {
		return nil, false
	}
	return get(btree.root, key)
}

func get(node *BTreeNode, key []byte) ([]byte, bool) {
	idx := sort.Search(node.num, func(i int) bool {
		return bytes.Compare(node.keys[i], key) >= 0
	})

	var found bool

	if idx < node.num && bytes.Compare(node.keys[idx], key) == 0 {
		found = true
	}

	if found {
		return node.values[idx], true
	}

	if node.isLeaf {
		return nil, false
	}

	return get(node.siblings[idx], key)

}

func (btree *BTree) Has(key []byte) bool {
	if btree.root == nil {
		return false
	}
	return has(btree.root, key)
}

func has(node *BTreeNode, key []byte) bool {
	idx := sort.Search(node.num, func(i int) bool {
		return bytes.Compare(node.keys[i], key) >= 0
	})

	var found bool

	if idx < node.num && bytes.Compare(node.keys[idx], key) == 0 {
		found = true
	}

	if found {
		return true
	}

	if node.isLeaf {
		return false
	}

	return has(node.siblings[idx], key)

}

func (tree *BTree) BFS() {
	if tree.root == nil {
		return
	}
	tree.root.bfs()
}

func (node *BTreeNode) bfs() {
	queue := append([]*BTreeNode{}, node)
	curLevelNums := 1
	nextLevelNums := node.num + 1
	var level int64
	for len(queue) > 0 {
		node := queue[0]
		curLevelNums--
		fmt.Printf("level=%d, keys=%v\n", level, node.keys[:node.num])
		if curLevelNums == 0 {
			level++
			curLevelNums = nextLevelNums
		} else {
			nextLevelNums += node.num + 1
		}
		queue = queue[1:]
		if !node.isLeaf {
			queue = append(queue, node.siblings[:node.num+1]...)
		}
	}
}

func assert(condition bool, msg ...string) {
	if !condition {
		panic(msg)
	}
}
