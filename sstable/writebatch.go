package sstable

import (
	"bytes"
	"encoding/binary"
	"sync"
)

type WriteBatch struct {
	count   int
	header  [kWriteBatchHeaderSize]byte
	scratch [binary.MaxVarintLen64]byte
	rep     bytes.Buffer
}

func (wb *WriteBatch) Put(key, value []byte) {

	wb.count++
	wb.rep.WriteByte(kTypeValue)
	n := binary.PutUvarint(wb.scratch[:], uint64(len(key)))
	wb.rep.Write(wb.scratch[:n])
	wb.rep.Write(key)

	n = binary.PutUvarint(wb.scratch[:], uint64(len(value)))
	wb.rep.Write(wb.scratch[:n])
	wb.rep.Write(value)
}

func (wb *WriteBatch) Delete(key InternalKey) {
	wb.count++
	wb.rep.WriteByte(kTypeDel)
	n := binary.PutUvarint(wb.scratch[:], uint64(len(key)))
	wb.rep.Write(wb.scratch[:n])
	wb.rep.Write(key)
}

func (wb *WriteBatch) SetSequence(seq Sequence) {
	binary.LittleEndian.PutUint64(wb.header[:8], uint64(seq))
}

func (wb *WriteBatch) Contents() []byte {
	binary.LittleEndian.PutUint32(wb.header[8:], uint32(wb.count))
	return append(wb.header[:], wb.rep.Bytes()...)
}

func (wb *WriteBatch) Reset() {
	wb.count = 0
	wb.rep.Reset()
}

func (wb *WriteBatch) Len() int {
	return wb.count
}

func (wb *WriteBatch) Size() int {
	return wb.rep.Len() + len(wb.header)
}

func (dst *WriteBatch) append(src *WriteBatch) {
	dst.count += src.count
	dst.rep.Grow(src.rep.Len() - kWriteBatchHeaderSize)
	dst.rep.Write(src.rep.Bytes()[kWriteBatchHeaderSize:])
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
