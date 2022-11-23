package sstable

import (
	"bytes"
	"sort"
)

type compaction1 struct {
	inputs [2]tFiles
	levels Levels

	cPtr compactPtr

	// compaction grandparent level
	gp                tFiles
	gpOverlappedBytes int
	gpOverlappedLimit int

	cmp BasicComparer
}

func (vSet *VersionSet) pickCompaction1() *compaction1 {
	sizeCompaction := vSet.current.cScore >= 1

	if !sizeCompaction {
		return nil
	}

	cLevel := vSet.current.cLevel
	level := vSet.current.levels[cLevel]

	inputs := make(tFiles, 0)

	cPtr := vSet.compactPtrs[cLevel]
	if cPtr.ikey != nil {

		idx := sort.Search(len(level), func(i int) bool {
			return vSet.cmp.Compare(level[i].iMax, cPtr.ikey) > 0
		})

		if idx < len(level) {
			inputs = append(inputs, level[idx])
		}
	}

	if len(inputs) == 0 {
		inputs = append(inputs, level[0])
	}

	return newCompaction1(inputs, cPtr, vSet.current.levels, vSet.cmp)
}

func newCompaction1(inputs tFiles, cPtr compactPtr, levels Levels, cmp BasicComparer) *compaction1 {

	c := &compaction1{
		inputs: [2]tFiles{inputs, nil},
		levels: levels,
		cPtr:   cPtr,
		cmp:    cmp,
	}
	c.expand()
	return c
}

func (c *compaction1) expand() {

	inputLevel := c.inputs[0]
	compactLevel := c.inputs[1]

	if c.cPtr.level == 0 {

		inputLevel = c.levels[0].getOverlapped()

	}

}

func (tFiles tFiles) getOverlapped1(dst tFiles, imin InternalKey, imax InternalKey, overlapped bool) {

	umin := imin.ukey()
	umax := imax.ukey()

	if overlapped {
		i := 0
		for ; i < len(tFiles); i++ {
			if tFiles[i].overlapped(imin, imax) {
				tMinR := bytes.Compare(tFiles[i].iMin.ukey(), umin)
				tMaxR := bytes.Compare(tFiles[i].iMax.ukey(), umax)

				if tMinR >= 0 && tMaxR <= 0 {
					dst = append(dst, tFiles[i])
				} else {
					i = 0
					dst = dst[:0]
					if tMinR < 0 {
						umin = tFiles[i].iMin.ukey()
					}
					if tMaxR > 0 {
						umax = tFiles[i].iMax.ukey()
					}
				}
			}
		}
	} else {

		var (
			begin int
			end   int
		)

		idx := sort.Search(len(tFiles), func(i int) bool {
			return bytes.Compare(tFiles[i].iMin.ukey(), umin) <= 0
		})

		if idx == 0 {
			begin = 0
		} else if idx < len(tFiles) && bytes.Compare(tFiles[idx].iMax.ukey(), umin) <= 0 {
			begin -= 1
		} else {
			begin = idx
		}

		idx = sort.Search(len(tFiles), func(i int) bool {
			return bytes.Compare(tFiles[i].iMax.ukey(), umax) >= 0
		})

		if idx == len(tFiles) {
			end = idx
		} else if idx < len(tFiles) && bytes.Compare(tFiles[idx].iMin.ukey(), umax) <= 0 {
			end = idx + 1
		} else {
			end = idx
		}

		assert(end >= begin)
		dst = append(dst, tFiles[begin:end]...)
	}

}

func (a tFile) overlapped(imin InternalKey, imax InternalKey) bool {
	if bytes.Compare(a.iMax.ukey(), imin.ukey()) < 0 ||
		bytes.Compare(a.iMin.ukey(), imax.ukey()) > 0 {
		return false
	}
	return true
}
