package sstable

import (
	"bytes"
	"encoding/binary"
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
	prevIKey         internalKey
	entries          int
	restarts         []int
	restartThreshold int
	offset           int
}

func (bw *blockWriter) append(ikey internalKey, value []byte) {

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

func (bw *blockWriter) writeEntry(ikey internalKey, value []byte) {

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

type tableBuilder struct {
}

// todo finish it
func (tableBuilder *tableBuilder) appendKV(ikey internalKey, value []byte) {

}

// todo finish it
func (tableBuilder *tableBuilder) fileSize() int {
	return 0
}

func getPrefixKey(prevIKey, ikey internalKey) []byte {

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
