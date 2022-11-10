package sstable

import (
	"sync"
)

type sequence uint64

type DB struct {
	mutex      sync.Mutex
	VersionSet *VersionSet

	seqNum sequence
}
