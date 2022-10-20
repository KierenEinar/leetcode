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
	err         error
	dest        *writableFile
	blockOffset int
}

func (jw *JournalWriter) Write(chunk []byte) (n int, err error) {

	if jw.err != nil {
		return 0, jw.err
	}

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

		blockRemain = kJournalBlockSize - (jw.blockOffset + journalBlockHeaderLen)

		if blockRemain < journalBlockHeaderLen {
			_ = jw.dest.append(make([]byte, blockRemain))
			jw.blockOffset = 0
			continue
		}

		if chunkRemain > blockRemain {
			effectiveWrite = blockRemain
		} else {
			effectiveWrite = chunkRemain
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

		if effectiveWrite > 0 {
			writeNums++
		}

		jw.err = jw.writePhysicalRecord(chunk[n:n+effectiveWrite], chunkType)
		if jw.err != nil {
			return 0, jw.err
		}
		n = n + effectiveWrite

	}

	return

}

func (jw *JournalWriter) writePhysicalRecord(data []byte, chunkType byte) error {
	avail := len(data)
	record := make([]byte, journalBlockHeaderLen)
	checkSum := crc32.ChecksumIEEE(data)
	binary.LittleEndian.PutUint32(record, checkSum)
	binary.LittleEndian.PutUint16(record[4:], uint16(avail))
	record[6] = chunkType
	jw.blockOffset += avail + journalBlockHeaderLen
	err := jw.dest.append(record)
	if err != nil {
		return err
	}
	err = jw.dest.append(data)
	if err != nil {
		return err
	}
	return jw.dest.flush()
}

type writableFile struct {
	w   Writer
	pos int
	buf [kWritableBufferSize]byte
}

func (w *writableFile) append(data []byte) error {

	writeSize := len(data)
	copySize := copy(w.buf[w.pos:], data)
	// buf can hold entire data
	if copySize == writeSize {
		return nil
	}

	// buf is full and still need to add the data
	// so just writer to file and clear the buf
	if err := w.flush(); err != nil {
		return err
	}

	// calculate remain write size
	writeSize -= copySize
	if writeSize <= kWritableBufferSize {
		n := copy(w.buf[:], data[copySize:])
		w.pos = n
		return nil
	}

	// otherwise, the data is too large, so write to file direct
	if _, err := w.w.Write(data); err != nil {
		return err
	}
	return nil
}

func (w *writableFile) flush() error {
	if w.pos == 0 {
		return nil
	}
	_, err := w.w.Write(w.buf[:w.pos])
	w.pos = 0
	if err != nil {
		return err
	}
	return nil
}

type JournalReader struct {
}

func (jr *JournalReader) Read([]byte) (n int, err error) {

}
