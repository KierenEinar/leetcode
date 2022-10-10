package sstable

import (
	"bytes"
	"encoding/binary"
	"sort"
)

type blockData struct {
	data               []byte
	restartPointOffset int
	restartPointNums   int
}

func (a InternalKey) compare(b InternalKey) int {
	au := a.ukey()
	bu := b.ukey()
	ukeyC := bytes.Compare(au, bu)
	if ukeyC > 0 {
		return 1
	} else if ukeyC < 0 {
		return -1
	} else {
		aSeq := a.seq()
		bSeq := b.seq()
		if aSeq < bSeq {
			return 1
		} else if aSeq > bSeq {
			return -1
		}
		return 0
	}
}

func (br *blockData) readRestartPoint(restartPoint int) (unShareKey InternalKey) {
	_, n := binary.Uvarint(br.data[restartPoint:])
	unShareKeyLen, m := binary.Uvarint(br.data[restartPoint+n:])
	_, k := binary.Uvarint(br.data[restartPoint+n+m:])
	unShareKey = br.data[restartPoint+n+m+k : restartPoint+n+m+k+int(unShareKeyLen)]
	return
}

func (br *blockData) SeekRestartPoint(key InternalKey) int {

	n := sort.Search(br.restartPointNums, func(i int) bool {
		restartPoint := binary.LittleEndian.Uint32(br.data[br.restartPointOffset : br.restartPointOffset+i*4])
		unShareKey := br.readRestartPoint(int(restartPoint))
		result := unShareKey.compare(key)
		return result > 0
	})

	if n == 0 {
		return 0
	}

	return n - 1
}
