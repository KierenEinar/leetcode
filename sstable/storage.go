package sstable

import "io"

type Writer interface {
	io.Writer
	io.WriterAt
	io.Closer
}
