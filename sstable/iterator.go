package sstable

import "sync/atomic"

type iterator interface {
	Releaser
	Seek(key InternalKey) bool
	SeekFirst() bool
	Next() bool
	Key() []byte
	Value() []byte
	Valid() error
}

type iteratorIndexer interface {
	iterator
	Get() iterator
}

type emptyIterator struct{}

func (ei *emptyIterator) Seek(key InternalKey) bool {
	return false
}

func (ei *emptyIterator) SeekFirst() bool {
	return false
}

func (ei *emptyIterator) Next() bool {
	return false
}

func (ei *emptyIterator) Key() []byte {
	return nil
}

func (ei *emptyIterator) Value() []byte {
	return nil
}

func (ei *emptyIterator) Ref() int32 {
	return 0
}

func (ei *emptyIterator) UnRef() int32 {
	return 0
}

func (ei *emptyIterator) Valid() error {
	return nil
}

type Releaser interface {
	Ref() int32
	UnRef() int32
}

type BasicReleaser struct {
	ref     int32
	OnClose func()
	OnRef   func()
	OnUnRef func()
}

func (br *BasicReleaser) Ref() int32 {
	if br.OnRef != nil {
		br.OnRef()
	}
	return atomic.AddInt32(&br.ref, 1)
}

func (br *BasicReleaser) UnRef() int32 {
	newInt32 := atomic.AddInt32(&br.ref, -1)
	if newInt32 < 0 {
		panic("duplicated UnRef")
	}
	if br.OnUnRef != nil {
		br.OnUnRef()
	}
	if newInt32 == 0 {
		if br.OnClose != nil {
			br.OnClose()
		}
	}
	return newInt32
}

type indexedIterator struct {
	indexed iteratorIndexer
	data    iterator
	err     error
	ikey    InternalKey
	value   []byte
}

func (iter *indexedIterator) clearData() {
	iter.ikey = iter.ikey[:0]
	iter.value = iter.value[:0]
}

func (iter *indexedIterator) Next() bool {

}
