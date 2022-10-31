package sstable

import (
	"container/list"
	"sync"
)

type VersionSet struct {
	mutex    sync.RWMutex
	versions *list.List
	current  *Version
}

type Version struct {
	*BasicReleaser
	levels []tFiles
	// compaction
	cScore int
	cLevel int
}

type vBuilder struct {
	vSet     *VersionSet
	base     *Version
	inserted [kLevelNum]map[uint64][]addTable
	deleted  [kLevelNum]map[uint64]struct{}
}

func newBuilder() *vBuilder {

}
