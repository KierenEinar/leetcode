package sstable

import (
	"container/list"
	"encoding/binary"
	"sync"
)

type VersionSet struct {
	mutex       sync.RWMutex
	versions    *list.List
	current     *Version
	compactPtrs [kLevelNum]compactPtr
}

type Version struct {
	*BasicReleaser
	levels []tFiles
	// compaction
	cScore int
	cLevel int
}

type vBuilder struct {
	vSet     *VersionSet
	base     *Version
	inserted [kLevelNum]*tFileSortedSet
	deleted  [kLevelNum]*uintSortedSet
}

func newBuilder(vSet *VersionSet, base *Version) *vBuilder {
	builder := &vBuilder{
		vSet: vSet,
		base: base,
	}
	for i := 0; i < kLevelNum; i++ {
		builder.inserted[i] = newTFileSortedSet()
	}
	for i := 0; i < kLevelNum; i++ {
		builder.deleted[i] = newUintSortedSet()
	}
	return builder
}

func (builder *vBuilder) apply(edit VersionEdit) {
	for level, cPtr := range edit.compactPtrs {
		builder.vSet.compactPtrs[level] = cPtr
	}
	for _, delTable := range edit.delTables {
		level, number := delTable.level, delTable.number
		builder.deleted[level].add(number)
	}
	for _, addTable := range edit.addedTables {
		level, number := addTable.level, addTable.number
		builder.deleted[level].remove(number)
		builder.inserted[level].add(addTable)
	}
}

func (builder *vBuilder) saveTo(v *Version) {

}

type uintSortedSet struct {
	*anySortedSet
}

func newUintSortedSet() *uintSortedSet {
	uset := &uintSortedSet{
		anySortedSet: &anySortedSet{
			tree:                  InitBTree(3),
			anySortedSetEncodeKey: encodeUint64ToBinary,
		},
	}
	return uset
}

func encodeUint64ToBinary(item interface{}) (bool, []byte) {

	num, ok := item.(uint64)
	if !ok {
		return false, nil
	}

	switch {
	case num < uint64(1<<16)-1:
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(num))
		return true, buf
	case num < uint64(1<<32)-1:
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(num))
		return true, buf
	default:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, num)
		return true, buf
	}
}

type tFileSortedSet struct {
	*anySortedSet
}

func newTFileSortedSet() *tFileSortedSet {
	tset := &tFileSortedSet{
		anySortedSet: &anySortedSet{
			tree:                  InitBTree(3),
			anySortedSetEncodeKey: encodeUint64ToBinary,
		},
	}
	return tset
}

func encodeTFileToBinary(item interface{}) (bool, []byte) {

	tfile, ok := item.(tFile)
	if !ok {
		return false, nil
	}

	num := tfile.fd.Num

	switch {
	case num < uint64(1<<16)-1:
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(num))
		return true, buf
	case num < uint64(1<<32)-1:
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(num))
		return true, buf
	default:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, num)
		return true, buf
	}
}

type anySortedSet struct {
	tree *BTree
	anySortedSetEncodeKey
}

type anySortedSetEncodeKey func(item interface{}) (bool, []byte)

func (set *anySortedSet) add(item interface{}) bool {
	ok, key := set.anySortedSetEncodeKey(item)
	if !ok {
		panic("anySortedSet add item encode failed, please check...")
	}
	if !set.tree.Has(key) {
		set.tree.Insert(key, nil)
		return true
	}
	return false
}

func (set *anySortedSet) remove(item interface{}) bool {
	ok, key := set.anySortedSetEncodeKey(item)
	if !ok {
		panic("anySortedSet remove item encode failed, please check...")
	}
	return set.tree.Remove(key)
}

func (set *anySortedSet) contains(item interface{}) bool {
	ok, key := set.anySortedSetEncodeKey(item)
	if !ok {
		panic("anySortedSet contains item encode failed, please check...")
	}
	return set.tree.Has(key)
}
