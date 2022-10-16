package sstable

import (
	"io"
)

type Writer interface {
	io.Writer
	io.WriterAt
	io.Closer
	Syncer
}

type Reader interface {
	io.Reader
	io.ReaderAt
	io.Closer
}

type Syncer interface {
	Sync() error
}

type Fd struct{}

type Storage interface {
	Open()
}
