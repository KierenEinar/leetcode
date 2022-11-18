package sstable

import (
	"bytes"
	"math/rand"
	"sync"
)

const (
	kMaxHeight = 12
	p          = 1 / 4
	kBranching = 4
)

type SkipList struct {
	level     int8
	rand      *rand.Rand
	seed      int64
	dummyHead *skipListNode
	tail      *skipListNode
	length    int
	size      int
	rw        sync.RWMutex
}

func NewSkipList(seed int64) *SkipList {
	return &SkipList{
		rand:      rand.New(rand.NewSource(seed)),
		seed:      seed,
		dummyHead: &skipListNode{},
	}
}

func (skl *SkipList) Put(key, value []byte) {

	updates := make([]*skipListNode, kMaxHeight)
	n := skl.dummyHead
	for i := skl.level - 1; i >= 0; i-- {
		for n.next(i) != nil && bytes.Compare(n.next(i).key, key) < 0 {
			n = n.next(i)
		}
		updates[i] = n
	}

	// if key exists, just update the value
	if updates[0] != nil && bytes.Compare(updates[0].next(0).key, key) == 0 {
		skl.size -= len(updates[0].value)
		skl.size += len(value)
		updates[0].value = append([]byte(nil), value...)
		return
	}

	level := skl.randLevel()

	for i := skl.level; i < level; i++ {
		updates[i] = skl.dummyHead
	}

	if level > skl.level {
		skl.level = level
	}

	newNode := &skipListNode{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
		level: skipListNodeLevel{
			maxLevel: level,
			next:     make([]*skipListNode, level),
		},
	}

	for l := 0; l < len(updates); l++ {
		updates[l].setNext(int8(l), newNode)
	}

	// update forward
	updateNextLevel0 := newNode.next(0)
	if updateNextLevel0 != nil {
		updateNextLevel0.backward = newNode
	} else {
		skl.tail = updateNextLevel0
	}

	if updates[0] != skl.dummyHead {
		newNode.backward = updates[0]
	}

	skl.size += newNode.size()
	skl.length++

}

func (skl *SkipList) Del(key []byte) bool {

	if skl.tail == nil || skl.dummyHead.next(0) == nil {
		return false
	}

	updates := skl.findLT(key)

	if bytes.Compare(updates[0].next(0).key, key) != 0 {
		return false
	}

	foundNode := updates[0].next(0)
	for i := foundNode.level.maxLevel - 1; i >= 0; i-- {
		updates[i].setNext(i, foundNode.next(0))
	}

	// update skl level if is empty
	var level = foundNode.level.maxLevel
	for ; skl.dummyHead.next(level-1) == nil; level-- {
	}
	skl.level = level

	// update forward
	prev := foundNode.backward
	next := foundNode.next(0)

	if next != nil {
		if prev != nil {
			next.backward = prev
		}
	} else {
		// foundNode is the last one, so if prev is not null(prev not link dummy head)
		skl.tail = prev
	}

	skl.length--
	skl.size -= foundNode.size()
	return true
}

func (skl *SkipList) Get(key []byte) ([]byte, error) {
	if n, found := skl.FindGreaterOrEqual(key); found == true {
		return n.value, nil
	}
	return nil, ErrNotFound
}

func (skl *SkipList) FindGreaterOrEqual(key []byte) (*skipListNode, bool) {
	n := skl.dummyHead
	var (
		hitLevel int8 = -1
	)
	for i := skl.level - 1; i >= 0; i-- {
		for ; n.next(i) != nil && bytes.Compare(n.next(i).key, key) < 0; n = n.next(i) {
		}
		if n.next(i) != nil && bytes.Compare(n.next(i).key, key) == 0 {
			hitLevel = i
			break
		}
	}
	if hitLevel >= 0 { // case found
		return n.next(hitLevel), true
	}
	next := n.next(0)
	if next != nil {
		return next, false
	}
	return nil, false
}

func (skl *SkipList) findLT(key []byte) [kMaxHeight]*skipListNode {

	updates := [kMaxHeight]*skipListNode{}
	n := skl.dummyHead
	for i := skl.level - 1; i >= 0; i-- {
		for n.next(i) != nil && bytes.Compare(n.next(i).key, key) < 0 {
			n = n.next(i)
		}
		updates[i] = n
	}

	return updates

}

// todo use arena instead
type skipListNode struct {
	key      []byte
	value    []byte
	level    skipListNodeLevel
	backward *skipListNode
}

func (node *skipListNode) setNext(i int8, n *skipListNode) {
	assert(i < node.level.maxLevel)
	next := node.level.next[i]
	node.level.next[i] = n
	n.level.next[i] = next
}

func (node *skipListNode) next(i int8) *skipListNode {
	assert(i < node.level.maxLevel)
	return node.level.next[i]
}

func (node *skipListNode) size() int {
	return len(node.key) + len(node.value)
}

type skipListNodeLevel struct {
	next     []*skipListNode
	maxLevel int8
}

// required mutex held
func (skl *SkipList) randLevel() int8 {
	height := int8(1)
	// n = (1/p)^kMaxHeight, n = 16m, p=1/4 => kMaxHeight=12
	for height < kMaxHeight {
		if skl.rand.Int()%kBranching == 1 {
			height++
		} else {
			break
		}
	}
	assert(height <= kMaxHeight)
	return height
}
