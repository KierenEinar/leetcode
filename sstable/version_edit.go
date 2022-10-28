package sstable

import (
	"encoding/binary"
)

const (
	kComparerName = iota + 1
	kLogNum
	kNextFileNum
	kSeqNum
	kCompact
	kDelTable
	kAddTable
)

type VersionEdit struct {
	scratch      [binary.MaxVarintLen64]byte
	rec          uint64
	comparerName string
	logNum       uint64
	nextFileNum  uint64
	lastSeq      uint64
	compactPtr   compactPtr
	delTables    []delTable
	addedTables  []addTable
}

type compactPtr struct {
	level int
	ikey  InternalKey
}

type delTable struct {
	level  int
	number uint64
}

type addTable struct {
	level  int
	size   int
	number uint64
	imin   InternalKey
	imax   InternalKey
}

func (edit *VersionEdit) hasRec(bitPos uint8) bool {
	// 01110 & 1 << 1 -> 01110 & 00010 ==
	return edit.rec&1<<bitPos == 1<<bitPos
}

func (edit *VersionEdit) setRec(bitPos uint8) {
	edit.rec |= 1 << bitPos
}

func (edit *VersionEdit) setCompareName(cmpName string) {
	edit.setRec(kComparerName)
	edit.comparerName = cmpName
}

func (edit *VersionEdit) setLogNum(logNum uint64) {
	edit.setRec(kLogNum)
	edit.logNum = logNum
}

func (edit *VersionEdit) setNextFile(nextFileNum uint64) {
	edit.setRec(kNextFileNum)
	edit.nextFileNum = nextFileNum
}

func (edit *VersionEdit) setLastSeq(seq uint64) {
	edit.setRec(kSeqNum)
	edit.lastSeq = seq
}

func (edit *VersionEdit) addCompactPtr(level int, ikey InternalKey) {
	edit.setRec(kCompact)
	edit.compactPtr = compactPtr{
		level: level,
		ikey:  ikey,
	}
}

func (edit *VersionEdit) addDelTable(level int, number uint64) {
	edit.setRec(kDelTable)
	edit.delTables = append(edit.delTables, delTable{
		level:  level,
		number: number,
	})
}

func (edit *VersionEdit) addNewTable(level, size int, fileNumber uint64, imin, imax InternalKey) {
	edit.setRec(kAddTable)
	edit.addedTables = append(edit.addedTables, addTable{
		level:  level,
		size:   size,
		number: fileNumber,
		imin:   imin,
		imax:   imax,
	})
}

func (edit *VersionEdit) EncodeTo(w Writer) (err error) {
	switch {
	case edit.hasRec(kComparerName):
		err = edit.writeHeader(w, kComparerName)
		if err != nil {
			return err
		}
		fallthrough
	case edit.hasRec(kLogNum):

	}
}

func (edit *VersionEdit) writeHeader(w Writer, typ int) error {
	return edit.encodeVarInt(w, uint64(typ))
}

func (edit *VersionEdit) encodeVarInt(w Writer, value uint64) error {
	x := binary.PutUvarint(edit.scratch[:], value)
	_, err := w.Write(edit.scratch[:x])
	return err
}

func (edit *VersionEdit) encodeString(w Writer, value string) error {
	size := len(value)
	err := edit.encodeVarInt(w, uint64(size))
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(value))
	return err
}
