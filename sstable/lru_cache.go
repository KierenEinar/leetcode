package sstable

import (
	"bytes"
	hash2 "hash"
)

const htInitSlots = uint32(1 << 2)

type Cache interface {
	SetCapacity(capacity uint64)
	Insert(key []byte, hash uint32, charge uint32, value interface{}, deleter func(key []byte, value interface{})) *LRUHandle
	Lookup(key []byte, hash uint32) *LRUHandle
	Erase(key []byte, hash uint32) *LRUHandle
	Prune()
}

type LRUHandle struct {
	nextHash *LRUHandle

	next *LRUHandle
	prev *LRUHandle

	hash  uint32
	ref   uint32
	value interface{}
	key   []byte

	deleter func(key []byte, value interface{})
	charge  uint32
}

type LRUCache struct {
}

type HandleTable struct {
	list   []*LRUHandle
	slots  uint32
	size   uint32
	hash32 hash2.Hash
}

func NewHandleTable(slots uint32, hash32 hash2.Hash) *HandleTable {
	realSlots := uint32(0)
	for i := htInitSlots; i < 32; i++ {
		if slots < 1<<i {
			realSlots = 1 << i
			break
		}
	}

	return &HandleTable{
		list:   make([]*LRUHandle, realSlots),
		hash32: hash32,
		slots:  realSlots,
		size:   0,
	}
}

func (ht *HandleTable) Insert(handle *LRUHandle) *LRUHandle {
	ptr := ht.FindPointer(handle.key, handle.hash)
	old := *ptr
	if old != nil {
		handle.nextHash = old
	}
	*ptr = handle
	ht.size++
	if ht.size > ht.slots {
		ht.Resize(true)
	}
	return old
}

func (ht *HandleTable) Lookup(key []byte, hash uint32) *LRUHandle {
	ptr := ht.FindPointer(key, hash)
	return *ptr
}

func (ht *HandleTable) Erase(key []byte, hash uint32) *LRUHandle {
	ptr := ht.FindPointer(key, hash)
	if *ptr != nil {
		ht.size--
		if ht.size < ht.slots>>1 && ht.slots > htInitSlots {
			ht.Resize(false)
		}
	}
	return *ptr
}

func (ht *HandleTable) FindPointer(key []byte, hash uint32) **LRUHandle {
	slot := hash & ht.slots
	ptr := &ht.list[slot]
	for *ptr != nil && (*ptr).hash != hash || bytes.Compare((*ptr).key, key) != 0 {
		ptr = &(*ptr).nextHash
	}
	return ptr
}

func (ht *HandleTable) Resize(growth bool) {

}
