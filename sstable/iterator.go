package sstable

import "sync/atomic"

type Iterator interface {
	Releaser
	Seek(key InternalKey) bool
	SeekFirst() bool
	Next() bool
	Key() []byte
	Value() []byte
	Valid() error
}

type iteratorIndexer interface {
	Iterator
	Get() Iterator
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
	released bool
	ref      int32
	OnClose  func()
	OnRef    func()
	OnUnRef  func()
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
			br.released = true
			br.OnClose()
		}
	}
	return newInt32
}

type indexedIterator struct {
	*BasicReleaser
	indexed iteratorIndexer
	data    Iterator
	err     error
	ikey    InternalKey
	value   []byte
}

func newIndexedIterator(indexed iteratorIndexer) Iterator {
	ii := &indexedIterator{
		indexed: indexed,
		BasicReleaser: &BasicReleaser{
			OnClose: func() {
				indexed.UnRef()
			},
		},
	}
	return ii
}

func (iter *indexedIterator) clearData() {
	if iter.data != nil {
		iter.data.UnRef()
		iter.data = nil
	}
}

func (iter *indexedIterator) setData() {
	iter.data = iter.indexed.Get()
}

func (iter *indexedIterator) Next() bool {

	if iter.err != nil {
		return false
	}

	if iter.released {
		iter.err = ErrReleased
		return false
	}

	if iter.data != nil && iter.data.Next() {
		return true
	}

	iter.clearData()

	if iter.indexed.Next() {
		iter.setData()
		return iter.Next()
	}

	return false
}

func (iter *indexedIterator) SeekFirst() bool {

	if iter.err != nil {
		return false
	}

	if iter.released {
		iter.err = ErrReleased
		return false
	}

	iter.clearData()
	if !iter.indexed.SeekFirst() {
		return false
	}

	iter.setData()
	return iter.Next()
}

func (iter *indexedIterator) Seek(key InternalKey) bool {
	if iter.err != nil {
		return false
	}

	if iter.released {
		iter.err = ErrReleased
		return false
	}

	iter.clearData()

	if !iter.indexed.Seek(key) {
		return false
	}

	iter.setData()

	return iter.data.Seek(key)

}

func (iter *indexedIterator) Key() []byte {
	if iter.data != nil {
		return iter.data.Key()
	}
	return nil
}

func (iter *indexedIterator) Value() []byte {
	if iter.data != nil {
		return iter.data.Value()
	}
	return nil
}

func (iter *indexedIterator) Valid() error {
	return iter.err
}

type MergeIterator struct {
}
