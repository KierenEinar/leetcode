package sstable

import (
	"errors"
	"fmt"
)

type ErrCorruption struct {
	error
}

func NewErrCorruption(msg string) *ErrCorruption {
	return &ErrCorruption{
		error: fmt.Errorf("leveldb/table err corruption, msg=%s", msg),
	}
}

var (
	ErrIterOutOfBounds          = errors.New("leveldb/table Iterator offset out of bounds")
	ErrIterInvalidSharedKey     = errors.New("leveldb/table Iterator invald shared key")
	ErrUnSupportCompressionType = errors.New("leveldb/table not support compression type")
	ErrNotFound                 = errors.New("leveldb err not found")
	ErrReleased                 = errors.New("leveldb released")
	ErrJournalSkipped           = errors.New("leveldb/journal skipped")
	ErrMissingChunk             = errors.New("leveldb/journal chunk miss")
	ErrClosed                   = errors.New("leveldb/shutdown")
	ErrFileIsDir                = errors.New("leveldb/path is dir")
	ErrDeleted                  = errors.New("leveldb/memdb key deleted")
)
