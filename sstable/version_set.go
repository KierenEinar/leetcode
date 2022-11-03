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

	for level := 0; level < kLevelNum; level++ {

	}

}

func upperBound(s tFiles, level int) {

}

type uintSortedSet struct {
	*anySortedSet
}

func newUintSortedSet() *uintSortedSet {
	uSet := &uintSortedSet{
		anySortedSet: &anySortedSet{
			BTree:                 InitBTree(3, &uint64Comparer{}),
			anySortedSetEncodeKey: encodeUint64ToBinary,
		},
	}
	return uSet
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

func decodeBinaryToUint64(b []byte) uint64 {

	size := len(b)
	var value uint64
	switch {
	case size == 2:
		value = uint64(binary.LittleEndian.Uint16(b))
	case size == 4:
		value = uint64(binary.LittleEndian.Uint32(b))
	case size == 8:
		value = binary.LittleEndian.Uint64(b)
	default:
		panic("unsupport type decode to uint64")
	}
	return value
}

type uint64Comparer struct{}

func (uc uint64Comparer) Compare(a, b []byte) int {
	uinta, uintb := decodeBinaryToUint64(a), decodeBinaryToUint64(b)
	if uinta < uintb {
		return -1
	} else if uinta == uintb {
		return 0
	} else {
		return 1
	}
}

type tFileSortedSet struct {
	*anySortedSet
}

func newTFileSortedSet() *tFileSortedSet {
	tSet := &tFileSortedSet{
		anySortedSet: &anySortedSet{
			BTree:                 InitBTree(3, &tFileComparer{}),
			anySortedSetEncodeKey: encodeTFileToBinary,
		},
	}
	return tSet
}

type tFileComparer struct {
	*iComparer
}

func (tc *tFileComparer) Compare(a, b []byte) int {

	ia := a[:len(a)-8]
	ib := a[:len(b)-8]
	r := tc.iComparer.Compare(ia, ib)
	if r != 0 {
		return r
	}

	if aNum, bNum := binary.LittleEndian.Uint64(a[len(a)-8:]), binary.LittleEndian.Uint64(b[len(b)-8:]); aNum < bNum {
		return -1
	}
	return 1
}

func encodeTFileToBinary(item interface{}) (bool, []byte) {
	tfile, ok := item.(tFile)
	if !ok {
		return false, nil
	}
	fileNum := make([]byte, 8)
	binary.LittleEndian.PutUint64(fileNum, tfile.fd.Num)
	key := append(tfile.iMax, fileNum...)
	return true, key
}

type anySortedSet struct {
	*BTree
	anySortedSetEncodeKey
}

type anySortedSetEncodeKey func(item interface{}) (bool, []byte)

func (set *anySortedSet) add(item interface{}) bool {
	ok, key := set.anySortedSetEncodeKey(item)
	if !ok {
		panic("anySortedSet add item encode failed, please check...")
	}
	if !set.Has(key) {
		set.Insert(key, nil)
		return true
	}
	return false
}

func (set *anySortedSet) remove(item interface{}) bool {
	ok, key := set.anySortedSetEncodeKey(item)
	if !ok {
		panic("anySortedSet remove item encode failed, please check...")
	}
	return set.Remove(key)
}

func (set *anySortedSet) contains(item interface{}) bool {
	ok, key := set.anySortedSetEncodeKey(item)
	if !ok {
		panic("anySortedSet contains item encode failed, please check...")
	}
	return set.Has(key)
}
