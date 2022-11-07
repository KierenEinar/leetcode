package sstable

import "sync"

type DB struct {
	mutex sync.Mutex
}
