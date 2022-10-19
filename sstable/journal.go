package sstable

import (
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
	fullChunk   = byte(1)
	firstChunk  = byte(2)
	middleChunk = byte(3)
	lastChunk   = byte(4)
)

type JournalWriter struct {
	w         Writer
	buf       [journalBlockSize]byte
	offset    int
	blockSize int
}

func (jw *JournalWriter) Write(chunk []byte) (n int, err error) {
	chunkLen := len(chunk)
	chunkRemain := chunkLen

	var (
		writeNums   int
		chunkType   byte
		blockRemain int
	)

	for {

		var (
			effectiveWrite int
		)

		if chunkRemain == 0 {
			break
		}

		blockRemain = journalBlockSize - (jw.offset + journalBlockHeaderLen)

		if blockRemain < journalBlockHeaderLen {
			for i := jw.offset; i < journalBlockSize; i++ {
				jw.buf[i] = 0
			}
			_, err = jw.w.Write(jw.buf[jw.offset:])
			if err != nil {
				return 0, err
			}
			jw.offset = 0
			continue
		}

		if chunkRemain > blockRemain {
			effectiveWrite = blockRemain
		} else {
			effectiveWrite = chunkRemain
		}

		if effectiveWrite == 0 {
			_ = jw.writeRecord(nil, fullChunk)
			continue
		}

		chunkRemain = chunkRemain - effectiveWrite

		if writeNums == 0 {
			if chunkRemain == 0 {
				chunkType = fullChunk
			} else {
				chunkType = firstChunk
			}
		} else {
			if chunkRemain == 0 {
				chunkType = lastChunk
			} else {
				chunkType = middleChunk
			}
		}

		writeNums++
		if err = jw.writeRecord(chunk[n:n+effectiveWrite], chunkType); err != nil {
			return n, err
		}

		n = n + effectiveWrite

	}

	return

}

func (jw *JournalWriter) Flush() error {
	return jw.w.Sync()
}

func (jw *JournalWriter) writeRecord(data []byte, chunkType byte) error {
	avail := len(data)
	record := make([]byte, journalBlockHeaderLen)
	checkSum := crc32.ChecksumIEEE(data)
	binary.LittleEndian.PutUint32(record, checkSum)
	binary.LittleEndian.PutUint16(record[4:], uint16(avail))
	record[6] = chunkType
	// write header
	copy(jw.buf[jw.offset:], record)
	jw.offset += journalBlockHeaderLen
	for {
		if avail == 0 {
			return nil
		}
		n := copy(jw.buf[jw.offset:], data)
		if err := jw.writeFile(jw.buf[jw.offset : jw.offset+n]); err != nil {
			jw.seekNextBlock() // seek writer to next block
			return err
		}
		jw.offset += n
		if jw.offset >= jw.blockSize {
			jw.offset -= jw.blockSize
		}
		avail -= n
		data = data[n:]
	}
}

func (jw *JournalWriter) writeFile(data []byte) error {
	avail := len(data)
	for {
		n, err := jw.w.Write(data)
		if err != nil {
			return err
		}
		avail -= n
		if avail == 0 {
			return nil
		}
	}
}

type JournalReader struct {
}
