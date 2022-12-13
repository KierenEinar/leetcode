package sstable

import (
	hash2 "hash"
	"runtime"
)

type TableCache struct {
	cache Cache
}

func (tc *TableCache) Close() {

}

func NewTableCache(capacity uint32, hash32 hash2.Hash32) *TableCache {
	c := &TableCache{
		cache: NewCache(capacity, hash32),
	}
	runtime.SetFinalizer(c, (*TableCache).Close)
	return c
}

func (tc *TableCache) Get(ikey InternalKey, tFile tFile, value *[]byte) error {

}
