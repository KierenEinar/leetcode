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
		error: fmt.Errorf("err corruption, msg=%s", msg),
	}
}

var (
	ErrIterOutOfBounds      = errors.New("leveldb/table iterator offset out of bounds")
	ErrIterInvalidSharedKey = errors.New("leveldb/table iterator invald shared key")
)
