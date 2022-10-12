package sstable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"sort"
)

type dataBlock struct {
	*BasicReleaser
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
	block := &dataBlock{
		data:               data,
		restartPointNums:   restartPointNums,
		restartPointOffset: restartPointOffset,
	}
	block.OnClose = block.Close
	block.Ref()
	return block, nil
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
	*BasicReleaser
	ref     int32
	offset  int
	prevKey []byte
	dir     direction
	err     error
	ikey    []byte
	value   []byte
}

func newBlockIter(dataBlock *dataBlock) *blockIter {
	bi := &blockIter{
		dataBlock: dataBlock,
	}
	br := &BasicReleaser{
		OnRef: func() {
			dataBlock.Ref()
		},
		OnUnRef: func() {
			dataBlock.UnRef()
		},
		OnClose: bi.Close,
	}
	bi.BasicReleaser = br
	bi.Ref()
	return bi
}

type direction int

const (
	dirSOI     direction = 1
	dirForward direction = 2
	dirEOI     direction = 3
)

func (bi *blockIter) Close() {
	bi.prevKey = nil
	bi.offset = 0
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
	tableSize   int
	metaBlock   *metaBlock
	indexBlock  *dataBlock
	indexBH     blockHandle
	metaIndexBH blockHandle
}

func newTableReader(r Reader, fileSize int) (*tableReader, error) {
	footer := make([]byte, tableFooterLen)
	_, err := r.ReadAt(footer, int64(fileSize-tableFooterLen))
	if err != nil {
		return nil, err
	}
	tr := &tableReader{
		r:         r,
		tableSize: fileSize,
	}
	err = tr.readFooter()
	if err != nil {
		return nil, err
	}
	return tr, nil
}

func (tableReader *tableReader) readFooter() error {
	footer := make([]byte, tableFooterLen)
	_, err := tableReader.r.ReadAt(footer, int64(tableReader.tableSize-tableFooterLen))
	if err != nil {
		return err
	}

	magic := footer[40:]
	if bytes.Compare(magic, magicByte) != 0 {
		return NewErrCorruption("footer decode failed")
	}

	bhLen, indexBH := readBH(footer)
	tableReader.indexBH = indexBH

	_, tableReader.metaIndexBH = readBH(footer[bhLen:])
	return nil
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

func (tr *tableReader) getIndexBlock() (*dataBlock, error) {
	if tr.indexBlock != nil {
		return tr.indexBlock, nil
	}
	b, err := tr.readRawBlock(tr.indexBH)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Seek return gte key
func (tr *tableReader) find(key InternalKey) (ikey InternalKey, value []byte, err error) {
	indexBlock, err := tr.getIndexBlock()
	if err != nil {
		return
	}
	defer indexBlock.UnRef()

	indexBlockIter := newBlockIter(indexBlock)
	defer indexBlockIter.UnRef()

	if !indexBlockIter.Seek(key) {
		err = ErrNotFound
		return
	}

	_, blockHandle := readBH(indexBlockIter.Value())

	dataBlock, err := tr.readRawBlock(blockHandle)
	if err != nil {
		return
	}
	defer dataBlock.UnRef()

	dataBlockIter := newBlockIter(dataBlock)
	defer dataBlockIter.UnRef()

	if dataBlockIter.Seek(key) {
		ikey = dataBlockIter.Key()
		value = append([]byte(nil), dataBlockIter.value...)
		return
	}

	/**
	special case
	0..block last  abcd
	1..block first abcz
	so the index block key is abce
	if search abce, so block..0 won't exist abce, should go to the next block
	*/

	if !indexBlockIter.Next() {
		err = ErrNotFound
		return
	}

	_, blockHandle1 := readBH(indexBlockIter.Value())

	dataBlock1, err := tr.readRawBlock(blockHandle1)
	if err != nil {
		return
	}
	dataBlock1.UnRef()

	dataBlockIter1 := newBlockIter(dataBlock1)
	defer dataBlockIter1.UnRef()

	if !dataBlockIter1.Seek(key) {
		err = ErrNotFound
		return
	}

	ikey = dataBlockIter1.Key()
	value = append([]byte(nil), dataBlockIter1.Value()...)
	return
}

type indexIter struct {
	*blockIter
	tr *tableReader
}

func (indexIter *indexIter) Get() iterator {
	value := indexIter.Value()
	if value == nil {
		return nil
	}

	_, bh := readBH(value)

	dataBlock, err := indexIter.tr.readRawBlock(bh)
	if err != nil {
		indexIter.err = err
		return nil
	}
	defer dataBlock.UnRef()

	return newBlockIter(dataBlock)

}

type metaBlock struct {
}
