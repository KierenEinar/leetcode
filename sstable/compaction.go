package sstable

import (
	"bytes"
	"encoding/binary"
	"sort"
)

const (
	defaultCompactionTableSize = 1 << 21 // 默认一个文件的大小是2m
	// 默认level0与level1文件合并的过程中, 产生的新文件与level2文件重叠个数如果超过10个的话, 就需要将写入文件重新生成一个
	defaultGPOverlappedLimit = 10

	// 默认合并s0如果扩大那么要检查s0+s1不能超过25个文件
	defaultCompactionExpandS0LimitFactor = 25

	// data block的size
	defaultDataBlockSize = 1 << 11 // 2k
)

func buildInternalKey(dst, uKey []byte, kt keyType, sequence uint64) InternalKey {
	dst = ensureBuffer(dst, len(dst)+8)
	n := copy(dst, uKey)
	binary.LittleEndian.PutUint64(dst[n:], (sequence<<8)|uint64(kt))
	return dst
}

func ensureBuffer(dst []byte, size int) []byte {
	if len(dst) < size {
		return make([]byte, size)
	}
	return dst[:size]
}

type Compaction struct {
	inputLevel  int
	cPtr        InternalKey
	sFiles      [2]sFiles
	levels      Levels
	tableSize   int
	tableWriter *tableWriter
	minSeq      uint64

	// grandparent overlapped
	gp                sFiles
	gpi               int
	gpOverlappedBytes int
	gpOverlappedLimit int
	seenKey           bool

	// approximately compact del key
	baseLevelI []int
}

func (fileMeta *FileMeta) pickCompaction() *Compaction {
	inputLevel := fileMeta.BestCLevel
	cPtr := fileMeta.loadCompactPtr(inputLevel)

	var s0 sFiles

	level := fileMeta.Levels[inputLevel]

	if cPtr != nil && inputLevel > 0 { // only level [1,n] can find the compact ptr

		n := sort.Search(len(level), func(i int) bool {
			return bytes.Compare(level[i].iMax, cPtr) > 0
		})
		if n < len(level) {
			s0 = append(s0, level[n])
		}
	}

	if len(s0) == 0 {
		s0 = append(s0, level[0])
	}

	return newCompaction(inputLevel, s0, fileMeta.Levels)

}

func (fileMeta *FileMeta) finishCompactionOutputFile(tableWriter *tableWriter) error {
	return nil
}

func (fileMeta *FileMeta) doCompaction(compaction *Compaction) error {

	var (
		hasCurrentUserKey   = false
		currentUserKey      []byte
		lastUserKeySequence = kMaxSequenceNum
		minSeq              = compaction.minSeq
	)

	iter := fileMeta.makeInputMergedIterator()

	for iter.next() {

		var drop = false

		ikey := iter.key()
		if compaction.tableWriter != nil && compaction.shouldStopBefore(ikey) {
			err := fileMeta.finishCompactionOutputFile(compaction.tableWriter)
			if err != nil {
				return err
			}
		}

		ukey, kt, useq, kerr := parseInternalKey(ikey)
		if kerr != nil {
			hasCurrentUserKey = false
			currentUserKey = nil
			lastUserKeySequence = kMaxSequenceNum
		} else {

			if !hasCurrentUserKey || bytes.Compare(ukey, currentUserKey) != 0 {
				currentUserKey = ukey
				hasCurrentUserKey = true
				lastUserKeySequence = kMaxSequenceNum
			}

			if lastUserKeySequence <= minSeq {
				drop = true
			} else if kt == keyTypeDel && useq <= minSeq && compaction.isBaseLevelForKey(currentUserKey) {
				drop = true
			}
			lastUserKeySequence = useq
		}

		if !drop {
			if compaction.tableWriter == nil {
				builder, err := fileMeta.createNewTable(defaultCompactionTableSize)
				if err != nil {
					return err
				}
				compaction.tableWriter = builder
			}

			compaction.tableWriter.appendKV(ikey, iter.value())
			if compaction.tableWriter.fileSize() >= defaultCompactionTableSize {
				cErr := fileMeta.finishCompactionOutputFile(compaction.tableWriter)
				if cErr != nil {
					return cErr
				}
			}
		}

	}
	return nil
}

func newCompaction(inputLevel int, s0 sFiles, levels Levels) *Compaction {
	c := &Compaction{
		inputLevel:        inputLevel,
		sFiles:            [2]sFiles{s0, nil},
		levels:            levels,
		tableSize:         defaultCompactionTableSize,
		gp:                make(sFiles, 0),
		gpOverlappedLimit: defaultGPOverlappedLimit * defaultCompactionTableSize,
		baseLevelI:        make([]int, len(levels)),
	}

	c.expand()
	return c
}

func (c *Compaction) expand() {

	var (
		s0, s1 = c.sFiles[0], c.sFiles[1]
		vs0    = c.levels[c.inputLevel]
		vs1    = sFiles{}
	)

	if c.inputLevel+1 < len(c.levels) {
		vs1 = c.levels[c.inputLevel+1]
	}

	imin, imax := s0.getRange()

	if c.inputLevel == 0 {
		s0 = vs0.getOverlapped(imin, imax, c.inputLevel == 0)
		imin, imax = s0.getRange()
	}

	s1 = vs1.getOverlapped(imin, imax, false)

	// recalculate imin imax
	imin, imax = append(s0, s1...).getRange()

	as0 := vs0.getOverlapped(imin, imax, c.inputLevel == 0)

	if len(as0) > len(s0) {
		// s0 get larger will check limit factor
		if as0.size()+s1.size() <= defaultCompactionExpandS0LimitFactor*defaultCompactionTableSize {
			amin, amax := append(as0, s1...).getRange()
			as1 := vs1.getOverlapped(amin, amax, false)
			if len(as1) == len(s1) { // s1 should not change, otherwise should recalculate and go into recursive
				s0 = as0
				imin, imax = amin, amax
			}
		}
	}

	// set this level0+level1 compaction overlapped size with grandparent
	if c.inputLevel+2 < len(c.levels) {
		c.gp = c.levels[c.inputLevel+2].getOverlapped(imin, imax, false)
	}

	c.sFiles[0], c.sFiles[1] = s0, s1

}

func (c *Compaction) shouldStopBefore(ikey InternalKey) bool {

	for i := c.gpi; i < len(c.gp); i++ {
		if bytes.Compare(ikey, c.gp[i].iMax) > 0 {
			c.gpOverlappedBytes += c.gp[i].Size
			c.seenKey = true
			c.gpi++
		} else {
			break
		}
	}

	if c.seenKey && c.gpOverlappedBytes >= c.gpOverlappedLimit {
		c.gpOverlappedBytes = 0
		c.seenKey = false
		return true
	}
	return false
}

// approximately overlapped key by each level
func (c *Compaction) isBaseLevelForKey(ikey InternalKey) bool {

	/**
									 |
									\|/
								    ikey
				|--------|		|---------|
			|-------|  |-----| |-----|      |------|
	  |---|   |----------------|    |---------|   |----------|

	**/

	for i := 0; i < len(c.baseLevelI); i++ {
		levelI := c.baseLevelI[i]
		l := c.levels[i]
		for lIdx := levelI; lIdx < len(l); lIdx++ {
			if bytes.Compare(l[lIdx].iMax, ikey) >= 0 {
				if bytes.Compare(l[lIdx].iMin, ikey) <= 0 {
					return false
				}
				break
			}
			c.baseLevelI[i]++
		}
	}

	return true

}

func (s sFiles) getRange() (imin InternalKey, imax InternalKey) {

	for i, sFile := range s {
		if i == 0 {
			imin, imax = sFile.iMin, sFile.iMax
			continue
		}
		if bytes.Compare(sFile.iMax, imax) > 0 {
			imax = sFile.iMax
		}
		if bytes.Compare(sFile.iMin, imin) < 0 {
			imin = sFile.iMin
		}
	}
	return
}
