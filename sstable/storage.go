package sstable

import (
	"fmt"
	"io"
)

type Writer interface {
	io.Writer
	io.WriterAt
	io.Closer
	Syncer
}

type Reader interface {
	io.ByteReader
	io.Reader
	io.ReaderAt
	io.Closer
}

type Syncer interface {
	Sync() error
}

type FileType int

const (
	Manifest FileType = 1 << iota
	SSTable
	Journal
	Current
)

type Fd struct {
	FileType
	Num uint64
}

func (fd Fd) String() string {

	switch fd.FileType {
	case Manifest:
		return fmt.Sprintf("MANIFEST-%06d", fd.Num)
	case Journal:
		return fmt.Sprintf("%06d.log", fd.Num)
	case SSTable:
		return fmt.Sprintf("%06d.ldb", fd.Num)
	default:
		return fmt.Sprintf("%x-%06d", fd.FileType, fd.Num)
	}

}

type Storage interface {

	// Open reader
	Open(fd Fd) (Reader, error)

	// Create Writer, if writer exists, then will truncate it
	Create(fd Fd) (Writer, error)

	// Remove remove fd
	Remove(fd Fd) error

	// Rename rename fd
	Rename(fd Fd) error
}
