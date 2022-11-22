package sstable

import (
	"encoding/binary"
	"sync"
)

type WriteBatch struct {
	seq     Sequence
	count   int
	scratch [binary.MaxVarintLen64]byte
	rep     []byte
	once    sync.Once
}

func (wb *WriteBatch) Put(key, value []byte) {

	wb.once.Do(func() {
		wb.rep = make([]byte, kWriteBatchHeaderSize)
	})

	wb.count++
	wb.rep = append(wb.rep, kTypeValue)
	n := binary.PutUvarint(wb.scratch[:], uint64(len(key)))
	wb.rep = append(wb.rep, wb.scratch[:n]...)
	wb.rep = append(wb.rep, key...)

	n = binary.PutUvarint(wb.scratch[:], uint64(len(value)))
	wb.rep = append(wb.rep, wb.scratch[:n]...)
	wb.rep = append(wb.rep, value...)
}

func (wb *WriteBatch) Delete(key []byte) {
	wb.count++
	wb.rep = append(wb.rep, kTypeDel)
	n := binary.PutUvarint(wb.scratch[:], uint64(len(key)))
	wb.rep = append(wb.rep, wb.scratch[:n]...)
	wb.rep = append(wb.rep, key...)
}

func (wb *WriteBatch) SetSequence(seq Sequence) {
	wb.seq = seq
	binary.LittleEndian.PutUint64(wb.rep[:8], uint64(seq))
}

func (wb *WriteBatch) Contents() []byte {
	binary.LittleEndian.PutUint32(wb.rep[8:], uint32(wb.count))
	return wb.rep[:]
}

func (wb *WriteBatch) Reset() {
	wb.count = 0
	wb.rep = wb.rep[:kWriteBatchHeaderSize] // resize to header
}

func (wb *WriteBatch) Len() int {
	return wb.count
}

func (wb *WriteBatch) Size() int {
	return len(wb.rep)
}

func (wb *WriteBatch) Capacity() int {
	return cap(wb.rep)
}

func (dst *WriteBatch) append(src *WriteBatch) {
	dst.count += src.count
	dst.rep = append(dst.rep, src.rep...)
}

type writer struct {
	batch *WriteBatch
	done  bool
	err   error
	cv    *sync.Cond
}

func newWriter(batch *WriteBatch, mutex *sync.RWMutex) *writer {
	return &writer{
		batch: batch,
		done:  false,
		cv:    sync.NewCond(mutex),
	}
}
