package sstable

import "bytes"

type BasicComparer interface {
	Compare(a, b []byte) int
}

type BytesComparer struct{}

func (bc BytesComparer) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

type iComparer struct {
	uCmp BasicComparer
}

func (ic iComparer) Compare(a, b []byte) int {
	ia, ib := InternalKey(a), InternalKey(b)
	r := ic.uCmp.Compare(ia.ukey(), ib.ukey())
	if r != 0 {
		return r
	}
	m, n := ia.seq(), ib.seq()
	if m < n {
		return 1
	}
	return -1
}

var DefaultComparer = &BytesComparer{}
