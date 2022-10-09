package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
)

/**
sstable detail

	/--------------------------------/
	|			data block 0 (2k)    |
	/--------------------------------/
	/--------------------------------/
	|		    data block 1         |
	/--------------------------------/
	/--------------------------------/
	|			data block n         |
	/--------------------------------/
	/--------------------------------/
	|		meta block (filter)      |
	/--------------------------------/
	/--------------------------------/
	| meta index block (filter type) |
	/--------------------------------/
	/--------------------------------/
	|	      index block            |
	/--------------------------------/
	/--------------------------------/
	|             footer             |
	/--------------------------------/

block detail

	/------------------------------------/----------------/----------------------/
	|	              data               | 4byte checksum |1byte compression type|
	/------------------------------------/----------------/----------------------/

data block entry

	/------------------------------------------------/
	|share klen|unsharekey len|vlen|unshare key|value|
	/------------------------------------------------/

data block entries

	|                 index group 0                |     index group 1           |    4byte     |    4byte   |   4byte  |
	/----------------------------------------------/-----------------------------/--------------/------------/----------/
	|	entry 0  |  entry 1  | ........|  entry 15 | entry 16 |.......| entry 31 |  rs offset0  | rs offset1 |  rs nums |
	/----------------------------------------------/-----------------------------/--------------/------------/----------/

meta block (each data block's would be in offset [k, k+1]  )

	of0		   of1        of2        of3        data offset's offset
	/----------/----------/----------/----------/-----/-----/-----/-----/-----/----/
	| filter 0 | filter 1 | filter 2 | filter 3 | of0 | of1 | of2 | of3 | dof | lg |
	/----------/----------/----------/----------/-----/-----/-----/-----/-----/----/

e.g.
      data block0     data block1            data block2
	|------------|---------------|------------------------------------------|
	/---------------------/----------------------/------------------------/
	       baselg

	of0		   of1        data offset's offset
	/----------/----------/-----/-----/-----/-----/----/
	| filter 0 | filter 1 | of0 | of1 |	of2 | dof | lg |
	/----------/----------/-----/-----/-----/-----/----/

	of0 -> of0 pos
    of1 -> of1 pos
	of2 -> of1 pos


	data0 [offset0, offset1]
	data1 [offset0, offset1]
	data2 [offset1, offset2]
meta index block

	/---------------------/------------------------/
	|  key(filter.bloom)  |		block handle       |
	/---------------------/------------------------/

footer

	/-----------------------------/
	|      index block handle     |  0-20byte(2 varint)
	/-----------------------------/
	/-----------------------------/
	|	 meta index block handle  |  0-20byte(2 varint)
	/-----------------------------/
	/-----------------------------/
	|          padding            |
	/-----------------------------/
	/-----------------------------/
	|		     magic            |   8byte
	/-----------------------------/
**/

type blockWriter struct {
	scratch          []byte
	data             bytes.Buffer
	prevIKey         InternalKey
	entries          int
	restarts         []int
	restartThreshold int
	offset           int
}

func (bw *blockWriter) append(ikey InternalKey, value []byte) {

	if bw.entries%bw.restartThreshold == 0 {
		bw.prevIKey = append([]byte(nil))
		bw.restarts = append(bw.restarts, bw.offset)
	}

	bw.writeEntry(ikey, value)
	bw.entries++

	bw.prevIKey = append([]byte(nil), ikey...)

}

func (bw *blockWriter) finish() {

	if bw.entries == 0 {
		bw.restarts = append(bw.restarts, 0)
	}

	bw.restarts = append(bw.restarts, len(bw.restarts))

	for _, v := range bw.restarts {
		buf4 := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf4, uint32(v))
		bw.data.Write(buf4)
	}
}

func (bw *blockWriter) bytesLen() int {
	restartsLen := len(bw.restarts)
	if restartsLen == 0 {
		restartsLen = 1
	}
	return bw.data.Len() + restartsLen*4 + 4
}

func (bw *blockWriter) writeEntry(ikey InternalKey, value []byte) {

	var (
		shareUKey     = getPrefixKey(bw.prevIKey, ikey)
		shareUKeyLen  = len(shareUKey)
		unShareKeyLen = len(ikey) - shareUKeyLen
		unShareKey    = ikey[unShareKeyLen:]
		vLen          = len(value)
	)

	s1 := binary.PutUvarint(bw.scratch, uint64(shareUKeyLen))
	n1, _ := bw.data.Write(bw.scratch[:s1])

	s2 := binary.PutUvarint(bw.scratch, uint64(unShareKeyLen))
	n2, _ := bw.data.Write(bw.scratch[:s2])

	s3 := binary.PutUvarint(bw.scratch, uint64(vLen))
	n3, _ := bw.data.Write(bw.scratch[:s3])

	n4, _ := bw.data.Write(unShareKey)

	n5, _ := bw.data.Write(value)

	bw.offset += n1 + n2 + n3 + n4 + n5

}

func (bw *blockWriter) reset() {
	bw.data.Reset()
	bw.prevIKey = nil
	bw.offset = 0
	bw.restarts = bw.restarts[:0]
	bw.entries = 0
}

type FilterWriter struct {
	data            bytes.Buffer
	offsets         []int
	nkeys           int
	baseLg          int
	filterGenerator IFilterGenerator
	numBitsPerKey   uint8
}

func (fw *FilterWriter) addKey(ikey InternalKey) {
	fw.filterGenerator.AddKey(ikey.ukey())
	fw.nkeys++
}

type blockHandle struct {
	offset uint64
	length uint64
}

func (bh *blockHandle) writeEntry(dest []byte) []byte {
	dest = ensureBuffer(dest, binary.MaxVarintLen64*2)
	n1 := binary.PutUvarint(dest, bh.offset)
	n2 := binary.PutUvarint(dest[n1:], bh.length)
	return dest[:n1+n2]
}

type TableWriter struct {
	writer     Writer
	dataBlock  *blockWriter
	indexBlock *blockWriter

	filterBlock *FilterWriter

	blockHandle *blockHandle
	prevKey     InternalKey
	offset      int
	entries     int

	scratch [50]byte // tail 20 bytes used to encode block handle
}

func (tableWriter *TableWriter) Append(ikey InternalKey, value []byte) error {

	dataBlock := tableWriter.dataBlock
	filterBlock := tableWriter.filterBlock

	if tableWriter.entries > 0 && bytes.Compare(dataBlock.prevIKey, ikey) > 0 {
		return errors.New("tableWriter Append ikey not sorted")
	}

	err := tableWriter.flushPendingBH(ikey)
	if err != nil {
		return err
	}

	dataBlock.append(ikey, value)

	filterBlock.addKey(ikey)

	if dataBlock.bytesLen() >= defaultDataBlockSize {
		bh, ferr := tableWriter.finishBlock(dataBlock)
		if ferr != nil {
			return ferr
		}
		tableWriter.blockHandle = bh
	}

	return nil
}

func (tableWriter *TableWriter) finishBlock(blockWriter *blockWriter) (*blockHandle, error) {

	w := tableWriter.writer

	offset := tableWriter.offset
	length := blockWriter.bytesLen()

	bh := &blockHandle{
		offset: uint64(offset),
		length: uint64(length),
	}

	blockWriter.finish()
	blockTail := make([]byte, 5)
	checkSum := crc32.ChecksumIEEE(blockWriter.data.Bytes())
	compressionType := compressionTypeNone
	binary.LittleEndian.PutUint32(blockTail, checkSum)
	blockTail[4] = byte(compressionType)

	n, _ := blockWriter.data.Write(blockTail)

	_, err := w.Write(blockWriter.data.Bytes())
	if err != nil {
		return nil, err
	}

	tableWriter.offset += n
	blockWriter.reset()

	filterWriter := tableWriter.filterBlock
	err = filterWriter.flush(tableWriter.offset)
	if err != nil {
		return nil, err
	}

	return bh, nil
}

func (tableWriter *TableWriter) flushPendingBH(ikey InternalKey) error {

	if tableWriter.blockHandle == nil {
		return nil
	}
	var separator []byte
	if len(ikey) == 0 {
		separator = iSuccessor(tableWriter.prevKey)
	} else {
		separator = iSeparator(tableWriter.prevKey, ikey)
	}
	indexBlock := tableWriter.indexBlock
	bhEntry := tableWriter.blockHandle.writeEntry(tableWriter.scratch[30:])
	indexBlock.append(separator, bhEntry)
	tableWriter.blockHandle = nil
	return nil
}

// todo finish it
func (tableWriter *TableWriter) fileSize() int {
	return 0
}

func iSuccessor(a InternalKey) (dest InternalKey) {
	au := a.ukey()
	destU := getSuccessor(au)
	dest = append(destU, kMaxNumBytes...)
	return
}

func iSeparator(a, b InternalKey) (dest InternalKey) {
	au, bu := a.ukey(), b.ukey()
	destU := getSeparator(au, bu)
	dest = append(destU, kMaxNumBytes...)
	return
}

// return the successor that Gte ikey
// e.g. abc => b
// e.g. 0xff 0xff abc => 0xff 0xff b
func getSuccessor(a []byte) (dest []byte) {
	for i := range a {
		c := a[i]
		if c < 0xff {
			dest = append(dest, a[:i+1]...)
			dest[len(dest)-1]++
			return
		}
	}
	dest = append(dest, a...)
	return
}

// return x that is gte a and lt b
func getSeparator(a, b []byte) (dest []byte) {
	i, n := 0, len(a)
	if n > len(b) {
		n = len(b)
	}

	for ; i < n && a[i] == b[i]; i++ {

	}

	if i == n {

	} else if c := a[i]; c < 0xff && c+1 < b[i] {
		dest = append(dest, a[:i+1]...)
		dest[len(dest)-1]++
		return
	}

	dest = append(dest, a...)
	return
}

func getPrefixKey(prevIKey, ikey InternalKey) []byte {

	prevUkey := prevIKey.ukey()
	uKey := ikey.ukey()

	size := len(prevUkey)
	if len(uKey) < size {
		size = len(uKey)
	}

	dest := make([]byte, size)

	var sharePrefixIndex = 0

	for sharePrefixIndex = 0; sharePrefixIndex < size; sharePrefixIndex++ {
		c1 := prevUkey[sharePrefixIndex]
		c2 := uKey[sharePrefixIndex]
		if c1 == c2 {
			dest[sharePrefixIndex] = c1
		} else {
			break
		}
	}

	return dest[:sharePrefixIndex]
}
