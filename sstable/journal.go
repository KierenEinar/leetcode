package sstable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

/**
journal:
using physical record
each block is 32kb, block header is
		checksum      len  type
	/--------------/------/--/
	|	  4B	   |  2B  |1B|
	/--------------/-----/--/

	type including lastType, middleType, firstType

	when type is lastType, that means current record is in last chunk, only occur when chunk split multi record or chunk not cross block
	when type is middleType, that means current record neither first nor last record in chunk
	when type is firstType, that means current record is in first record and current chunk cross block


	| 		chunk0 		|					chunk1				|			chunk2			|   chunk3   |
	/--------------------------/-------------------------/-----------------------/-----------------------/
	|	                       |                         |                  	 |						 |
	/--------------------------/-------------------------/-----------------------/-----------------------/

**/

const (
	lastType   = byte(0)
	middleType = byte(1)
	firstType  = byte(2)
)

type JournalWriter struct {
	w         Writer
	block     bytes.Buffer
	offset    int
	blockSize int
}

func (jw *JournalWriter) Write(chunk []byte) (n int, err error) {
	w := jw.w
	chunkLen := len(chunk)
	blockRemain := jw.blockSize - jw.offset
	chunkRemain := chunkLen

	defer func() {

	}()

	var (
		offset         int
		effectiveWrite int
		writeNums      int
		blockType      byte
	)

	for {

		if chunkRemain == 0 {
			break
		}

		if blockRemain == 0 {
			jw.block.Reset()
			jw.offset = 0
			blockRemain = jw.blockSize
			continue
		}

		if blockRemain == journalBlockHeaderLen {
			_, err := jw.writeRecord(nil, lastType)
			if err != nil {
				return 0, err
			}
			blockRemain = 0
			continue
		}

		if chunkRemain > blockRemain {
			effectiveWrite = blockRemain
			chunkRemain = effectiveWrite - blockRemain
		} else {
			effectiveWrite = chunkRemain
			chunkRemain = 0
		}

		if writeNums == 0 {
			if chunkRemain == 0 {
				blockType = lastType
			} else {
				blockType = firstType
			}
		} else {
			if chunkRemain == 0 {
				blockType = lastType
			} else {
				blockType = middleType
			}
		}

		writeNums++
		_, err = jw.writeRecord(chunk[offset:offset+effectiveWrite], blockType)
		if err != nil {
			return offset, err
		}
		offset += effectiveWrite
	}
}

func (jw *JournalWriter) writeRecord(data []byte, blocType byte) (int, error) {
	emptyChunk := make([]byte, journalBlockHeaderLen)
	checkSum := crc32.ChecksumIEEE(data)
	binary.LittleEndian.PutUint32(emptyChunk, checkSum)
	binary.LittleEndian.PutUint16(emptyChunk[4:], 0)
	emptyChunk[6] = blocType

	_, err := jw.block.Write(emptyChunk)
	if err != nil {
		return 0, err
	}
	n, err := jw.block.Write(data)
	if err != nil {
		return 0, err
	}
	return n, nil
}

type JournalReader struct {
}
