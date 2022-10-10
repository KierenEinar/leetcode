package sstable

import "io"

type Writer interface {
	io.Writer
	io.WriterAt
	io.Closer
	Syncer
}

type Syncer interface {
	Sync() error
}
