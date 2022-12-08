package sstable

import (
	"bytes"
	hash2 "hash"
	"hash/fnv"
	"sync"
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

	inCache bool
	deleter func(key []byte, value interface{})
	charge  uint32

	cache *LRUCache
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
		handle.nextHash = old.nextHash
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
	old := *ptr
	if old != nil {
		ht.size--
		*ptr = old.next
		if ht.size < ht.slots>>1 && ht.slots > htInitSlots {
			ht.Resize(false)
		}
	}
	return old
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

	newSlots := ht.slots
	if growth {
		newSlots = newSlots << 1
	} else {
		newSlots = newSlots >> 1
		assert(newSlots >= htInitSlots)
	}

	newList := make([]*LRUHandle, newSlots)

	for i := uint32(0); i < ht.slots; i++ {
		ptr := &ht.list[i]
		for *ptr != nil {
			head := &newList[(*ptr).hash&newSlots]
			next := (*ptr).nextHash
			if *head != nil {
				(*ptr).nextHash = *head
			}
			*head = *ptr
			ptr = &next
		}
	}

	ht.list = newList

}

type LRUCache struct {
	rwMutex sync.RWMutex
	table   *HandleTable

	capacity uint32
	usage    uint32

	// dummy head
	inUse LRUHandle

	// dummy head
	lru LRUHandle
}

func (cache *LRUCache) NewCache(capacity uint32) *LRUCache {
	c := &LRUCache{
		capacity: capacity,
		table:    NewHandleTable(uint32(1<<8), fnv.New32()),
		inUse:    LRUHandle{},
		lru:      LRUHandle{},
	}

	c.inUse.next = &c.inUse
	c.inUse.prev = &c.inUse

	c.lru.next = &c.lru
	c.lru.prev = &c.lru

	return c

}

func (cache *LRUCache) Insert(key []byte, hash uint32, charge uint32,
	value interface{}, deleter func(key []byte, value interface{})) *LRUHandle {

	cache.rwMutex.Lock()
	defer cache.rwMutex.Unlock()

	handle := &LRUHandle{
		hash:    hash,
		ref:     1,
		value:   value,
		key:     append([]byte(nil), key...),
		inCache: true,
		deleter: deleter,
		charge:  charge,
	}

	handle.inCache = true
	handle.ref++
	cache.usage += charge
	lruAppend(&cache.inUse, handle)
	cache.finishErase(cache.table.Insert(handle))

	for cache.usage > cache.capacity && cache.lru.next != &cache.lru {
		cache.finishErase(cache.table.Erase(cache.lru.next.key, cache.lru.next.hash))
	}

	return handle

}

func lruAppend(lru *LRUHandle, h *LRUHandle) {
	h.next = lru
	lru.prev.next = h
	h.prev = lru.prev
	lru.prev = h
}

func lruRemove(h *LRUHandle) {
	h.prev.next = h.next
	h.next.prev = h.prev
}

func (lruCache *LRUCache) finishErase(h *LRUHandle) {
	if h != nil {
		h.inCache = false
		lruRemove(h)
		lruCache.usage -= h.charge
		h.UnRef()
	}
}

func (h *LRUHandle) UnRef() {

	assert(h.ref > 0)
	h.ref--
	if h.ref == 0 {
		h.deleter(h.key, h.value)
		h.cache = nil
	} else if h.ref == 1 && h.inCache {
		lruRemove(h)
		lruAppend(&h.cache.lru, h)
	}
}
