package sstable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
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
	kRecordFull    = byte(1)
	kRecordFirst   = byte(2)
	kRecordMiddle  = byte(3)
	kRecordLast    = byte(4)
	kRecordMaxType = kRecordLast
	kBadRecord     = kRecordMaxType + 1
	kEof           = kRecordMaxType + 2
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
				chunkType = kRecordFull
			} else {
				chunkType = kRecordFirst
			}
		} else {
			if chunkRemain == 0 {
				chunkType = kRecordLast
			} else {
				chunkType = kRecordMiddle
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
	jw.blockOffset += journalBlockHeaderLen
	err := jw.dest.append(record)
	if err != nil {
		return err
	}
	jw.blockOffset += avail
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
		w.pos += copySize
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

// JournalReader journal reader
// usage:
//	jr := JournalReader{}
//	for {
//		chunkReader, err := jr.NextChunk()
//		if err == io.EOF {
//			return
//		}
//		if err != nil {
//			return err
//		}
//		chunk, err:= ioutil.ReadAll(chunkReader)
//		if err == io.EOF {
//			return
//		}
//	    if err == ErrSkip {
//	   		continue
//	    }
//		if err != nil {
//			return err
//		}
//		process chunk
//	}
type JournalReader struct {
	src     *sequentialFile
	scratch bytes.Buffer // for reused read
}

type chunkReader struct {
	jr               *JournalReader
	inFragmentRecord bool // current fragment is part of chunk ?
	eof              bool
}

func (jr *JournalReader) NextChunk() (io.Reader, error) {

	for {
		recordType, fragment := jr.src.readPhysicalRecord()
		switch recordType {
		case kEof:
			return nil, io.EOF
		case kBadRecord, kRecordMiddle, kRecordLast: // drop whole block
			jr.scratch.Reset()
			continue
		case kRecordFull:
			jr.scratch.Write(fragment)
			return &chunkReader{jr, false, true}, nil
		case kRecordFirst:
			jr.scratch.Write(fragment)
			return &chunkReader{jr, true, false}, nil
		}
	}
}

func (chunk *chunkReader) Read(p []byte) (nRead int, rErr error) {

	jr := chunk.jr
	for {
		if jr.scratch.Len() == 0 && chunk.eof {
			return 0, io.EOF
		}

		n, _ := jr.scratch.Read(p)

		nRead += n

		// p is fill full
		if n == cap(p) {
			return nRead, nil
		}

		// p is not fill full, only if there has next chunk should read next chunk
		if jr.scratch.Len() == 0 && !chunk.eof {

			_, err := chunk.nextPartOfChunk()
			if err == io.EOF {
				chunk.eof = true
				return nRead, nil
			}

			if err == ErrJournalSkipped {
				jr.scratch.Reset()
				return nRead, ErrJournalSkipped
			}

			if err != nil {
				jr.scratch.Reset()
				return nRead, err
			}

			continue

		}
	}
}

func (chunk *chunkReader) nextPartOfChunk() (n int, err error) {
	if chunk.eof {
		return 0, io.EOF
	}

	recordType, fragment := chunk.jr.src.readPhysicalRecord()
	switch recordType {
	case kBadRecord:
		return 0, ErrJournalSkipped
	case kEof:
		return 0, io.EOF
	case kRecordFirst, kRecordFull:
		return 0, ErrJournalSkipped
	case kRecordMiddle, kRecordLast:
		if !chunk.inFragmentRecord {
			return 0, ErrJournalSkipped
		}
		n, _ = chunk.jr.scratch.Write(fragment)
		return n, nil
	default:
		return 0, ErrJournalSkipped
	}

}

type sequentialFile struct {
	r                  Reader
	physicalReadOffset int // current cursor read offset
	physicalN          int // current physical offset
	buf                [kJournalBlockSize]byte
	eof                bool
}

func (s *sequentialFile) readPhysicalRecord() (kRecordType byte, fragment []byte) {

	for {
		if s.physicalReadOffset+journalBlockHeaderLen > s.physicalN {
			if !s.eof {
				n, err := s.r.Read(s.buf[:])
				s.physicalN += n
				if err != nil {
					s.eof = true
					kRecordType = kEof
					return
				}
				if n < kJournalBlockSize {
					s.eof = true
				}
				continue
			} else {
				kRecordType = kEof
				return
			}
		}

		expectedSum := binary.LittleEndian.Uint32(s.buf[s.physicalReadOffset : s.physicalReadOffset+4])
		dataLen := int(binary.LittleEndian.Uint16(s.buf[s.physicalReadOffset+4 : s.physicalReadOffset+6]))
		kRecordType = s.buf[s.physicalReadOffset+6]

		if dataLen+s.physicalReadOffset > s.physicalN {
			kRecordType = kBadRecord
			s.physicalReadOffset = s.physicalN // drop whole record
			return
		}

		actualSum := crc32.ChecksumIEEE(s.buf[s.physicalReadOffset+journalBlockHeaderLen : s.physicalReadOffset+journalBlockHeaderLen+dataLen])
		if expectedSum != actualSum {
			kRecordType = kBadRecord
			s.physicalReadOffset = s.physicalN // drop whole record
			return
		}

		s.physicalReadOffset += dataLen

		// last empty block
		if dataLen == 0 {
			continue
		}

		return

	}
}
