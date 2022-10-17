package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"
)

const kMaxSequenceNum = (uint64(1) << 56) - 1
const kMaxNum = kMaxSequenceNum | uint64(keyTypeValue)

var magicByte = []byte("\x57\xfb\x80\x8b\x24\x75\x47\xdb")

const blockTailLen = 5
const tableFooterLen = 48

var (
	kMaxNumBytes = make([]byte, 8)
)

func init() {
	binary.PutUvarint(kMaxNumBytes, kMaxNum)
}

type InternalKey []byte

type CompressionType uint8

const (
	compressionTypeNone   CompressionType = 0
	compressionTypeSnappy CompressionType = 1
)

var defaultFilter = NewBloomFilter(10)

func (ik InternalKey) assert() {
	if len(ik) < 8 {
		panic("invalid internal key")
	}
}

func (ik InternalKey) ukey() []byte {
	ik.assert()
	dst := make([]byte, len(ik)-8)
	copy(dst, ik[:len(ik)-8])
	return dst
}

func (ik InternalKey) seq() uint64 {
	ik.assert()
	x := binary.LittleEndian.Uint64(ik[len(ik)-8:])
	return x >> 8
}

func (ik InternalKey) keyType() keyType {
	ik.assert()
	x := binary.LittleEndian.Uint64(ik[len(ik)-8:])
	kt := uint8(x & 1 << 7)
	return keyType(kt)
}

func parseInternalKey(ikey InternalKey) (ukey []byte, kt keyType, seq uint64, err error) {
	if len(ikey) < 8 {
		err = errors.New("invalid internal ikey len")
		return
	}

	num := binary.LittleEndian.Uint64(ikey[len(ikey)-8:])
	seq, kty := num>>8, num&0xff
	kt = keyType(kty)
	if kt > keyTypeDel {
		err = errors.New("invalid internal ikey keytype")
		return
	}
	return
}

type keyType uint8

const (
	keyTypeValue keyType = 0
	keyTypeDel   keyType = 1
)

type tFile struct {
	fd   Fd
	iMax InternalKey
	iMin InternalKey
	Size int
}

type tFiles []tFile

func (sf tFiles) size() (size int) {
	for _, v := range sf {
		size += v.Size
	}
	return
}

type Levels []tFiles

type FileMeta struct {
	NextSequence   uint64
	Levels         Levels
	NextFileNum    uint64
	CompactPtrs    []InternalKey // 合并的指针
	BestCScore     float64       // 最佳合并层的分数
	BestCLevel     int           // 最佳合并层的
	Storage        Storage
	tableOperation *tableOperation
}

func (fileMeta *FileMeta) loadCompactPtr(level int) InternalKey {
	if level < len(fileMeta.CompactPtrs) {
		return nil
	}
	return fileMeta.CompactPtrs[level]
}

func (s tFile) isOverlapped(umin []byte, umax []byte) bool {
	smin, smax := s.iMin.ukey(), s.iMax.ukey()
	return !(bytes.Compare(smax, umin) < 0) && !(bytes.Compare(smin, umax) > 0)
}

func (s tFiles) getOverlapped(imin InternalKey, imax InternalKey, overlapped bool) (dst tFiles) {

	if !overlapped {

		var (
			umin, umax        = imin.ukey(), imax.ukey()
			smallest, largest int
			sizeS             = len(s)
		)

		// use binary search begin
		n := sort.Search(sizeS, func(i int) bool {
			return bytes.Compare(s[i].iMin.ukey(), umin) >= 0
		})

		if n == 0 {
			smallest = 0
		} else if bytes.Compare(s[n-1].iMax.ukey(), umin) >= 0 {
			smallest = n - 1
		} else {
			smallest = sizeS
		}

		n = sort.Search(sizeS, func(i int) bool {
			return bytes.Compare(s[i].iMax.ukey(), umax) >= 0
		})

		if n == sizeS {
			largest = sizeS
		} else if bytes.Compare(s[n].iMin.ukey(), umax) >= 0 {
			largest = n + 1
		} else {
			largest = n
		}

		if smallest >= largest {
			return
		}

		dst = make(tFiles, largest-smallest)
		copy(dst, s[smallest:largest])
		return
	}

	var (
		i          = 0
		restart    = false
		umin, umax = imin.ukey(), imax.ukey()
	)

	for i < len(s) {
		sFile := s[i]
		if sFile.isOverlapped(umin, umax) {
			if bytes.Compare(sFile.iMax.ukey(), umax) > 0 {
				umax = sFile.iMax.ukey()
				restart = true
			}
			if bytes.Compare(sFile.iMin.ukey(), umin) < 0 {
				umin = sFile.iMin.ukey()
				restart = true
			}
			if restart {
				dst = dst[:0]
				i = 0
				restart = false // reset
			} else {
				dst = append(dst, sFile)
			}
		}
	}
	return
}

// todo finish it
func (fileMeta *FileMeta) createNewTable(fd Fd, fileSize int) (*TableWriter, error) {
	return nil, nil
}

type tableOperation struct {
	storage Storage
}

func newTableOperation(s Storage) *tableOperation {
	return &tableOperation{
		storage: s,
	}
}

func (tableOperation *tableOperation) open(f tFile) (*TableReader, error) {
	reader, err := tableOperation.storage.Open(f.fd)
	if err != nil {
		return nil, err
	}
	return NewTableReader(reader, f.Size)
}

func (tableOperation *tableOperation) newIterator(f tFile) (Iterator, error) {

	tr, err := tableOperation.open(f)
	if err != nil {
		return nil, err
	}
	return tr.NewIterator()
}
