package sstable

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
