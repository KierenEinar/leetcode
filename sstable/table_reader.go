package sstable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"sort"
	"sync/atomic"
)

type dataBlock struct {
	data               []byte
	restartPointOffset int
	restartPointNums   int
}

func newDataBlock(data []byte) (*dataBlock, error) {
	dataLen := len(data)
	if dataLen < 4 {
		return nil, NewErrCorruption("block data corruption")
	}
	restartPointNums := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	restartPointOffset := len(data) - (restartPointNums+1)*4
	return &dataBlock{
		data:               data,
		restartPointNums:   restartPointNums,
		restartPointOffset: restartPointOffset,
	}, nil
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

func (br *dataBlock) entry(offset int) (entryLen, shareKeyLen int, unShareKey, value []byte, err error) {
	if offset >= br.restartPointOffset {
		err = ErrIterOutOfBounds
		return
	}
	shareKeyLenU, n := binary.Uvarint(br.data[offset:])
	shareKeyLen = int(shareKeyLenU)
	unShareKeyLenU, m := binary.Uvarint(br.data[offset+n:])
	unShareKeyLen := int(unShareKeyLenU)
	vLenU, k := binary.Uvarint(br.data[offset+n+m:])
	vLen := int(vLenU)
	unShareKey = br.data[offset+n+m+k : offset+n+m+k+unShareKeyLen]
	value = br.data[offset+n+m+k+unShareKeyLen : offset+n+m+k+unShareKeyLen+vLen]
	entryLen = n + m + k + unShareKeyLen + vLen
	return
}

func (br *dataBlock) readRestartPoint(restartPoint int) (unShareKey InternalKey) {
	_, n := binary.Uvarint(br.data[restartPoint:])
	unShareKeyLen, m := binary.Uvarint(br.data[restartPoint+n:])
	_, k := binary.Uvarint(br.data[restartPoint+n+m:])
	unShareKey = br.data[restartPoint+n+m+k : restartPoint+n+m+k+int(unShareKeyLen)]
	return
}

func (br *dataBlock) SeekRestartPoint(key InternalKey) int {

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

func (br *dataBlock) Close() {
	br.data = br.data[:0]
	br.data = nil
	br.restartPointNums = 0
	br.restartPointOffset = 0
}

type blockIter struct {
	*dataBlock
	ref     int32
	offset  int
	prevKey []byte
	dir     direction
	err     error
	ikey    []byte
	value   []byte
}

type direction int

const (
	dirSOI     direction = 1
	dirForward direction = 2
	dirEOI     direction = 3
)

func (bi *blockIter) Ref() int32 {
	return atomic.AddInt32(&bi.ref, 1)
}

func (bi *blockIter) Close() {
	bi.prevKey = nil
	bi.offset = 0
}

func (bi *blockIter) UnRef() int32 {
	newInt32 := atomic.AddInt32(&bi.ref, -1)
	if newInt32 == 0 {
		bi.dataBlock.Close()
		bi.Close()
	}
	if newInt32 < 0 {
		panic("duplicated UnRef")
	}
	return newInt32
}

func (bi *blockIter) SeekFirst() bool {
	bi.dir = dirSOI
	bi.offset = 0
	bi.prevKey = bi.prevKey[:0]
	return bi.Next()
}

func (bi *blockIter) Seek(key InternalKey) bool {

	bi.offset = bi.SeekRestartPoint(key)
	bi.prevKey = bi.prevKey[:0]

	for bi.Next() {
		if bi.Valid() != nil {
			return false
		}
		ikey := InternalKey(bi.ikey)
		if ikey.compare(key) >= 0 {
			return true
		}
	}
	return false
}

func (bi *blockIter) Next() bool {

	if bi.offset >= bi.dataBlock.restartPointOffset {
		bi.dir = dirEOI
		return false
	}

	bi.dir = dirForward
	entryLen, shareKeyLen, unShareKey, value, err := bi.entry(bi.offset)
	if err != nil {
		bi.err = err
		return false
	}

	if len(bi.prevKey) < shareKeyLen {
		bi.err = ErrIterInvalidSharedKey
		return false
	}

	ikey := append(bi.prevKey[:shareKeyLen], unShareKey...)
	bi.ikey = ikey
	bi.value = value

	bi.offset = bi.offset + entryLen
	return true
}

func (bi *blockIter) Valid() error {
	return bi.err
}

func (bi *blockIter) Key() []byte {
	return bi.ikey
}

func (bi *blockIter) Value() []byte {
	return bi.value
}

type tableReader struct {
	r           Reader
	metaBlock   *metaBlock
	indexBlock  *dataBlock
	indexBH     blockHandle
	metaIndexBH blockHandle
}

// todo used cache
func (tableReader *tableReader) readRawBlock(bh blockHandle) (*dataBlock, error) {
	r := tableReader.r

	data := make([]byte, bh.length+blockTailLen)

	n, err := r.ReadAt(data, int64(bh.offset))
	if err != nil {
		return nil, err
	}

	rawData := data[:n-5]
	checkSum := binary.LittleEndian.Uint32(data[n-5 : n-1])
	compressionType := CompressionType(data[n-1])

	if crc32.ChecksumIEEE(rawData) != checkSum {
		return nil, NewErrCorruption("checksum failed")
	}

	switch compressionType {
	case compressionTypeNone:
	default:
		return nil, ErrUnSupportCompressionType
	}

	block, err := newDataBlock(data[:n])
	if err != nil {
		return nil, err
	}
	return block, nil
}

type metaBlock struct {
}
