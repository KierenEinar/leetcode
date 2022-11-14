package sstable

type MemTable struct {
	Iterator
}

func NewMemTable() *MemTable {
	panic("impl me")
}

func (memTable *MemTable) Put(ikey InternalKey, value []byte) error {
	panic("impl me")
}

func (memTable *MemTable) Get(ikey InternalKey) ([]byte, error) {
	panic("impl me")
}

func (memTable *MemTable) Has(ikey InternalKey) (bool, error) {
	panic("impl me")
}

func (memTable *MemTable) ApproximateSize() int {
	panic("impl me")
}
