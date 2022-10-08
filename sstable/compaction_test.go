package sstable

import (
	"testing"
)

func Test_ensureBuffer(t *testing.T) {

	var ik []byte
	start := 10
	for i := 0; i < 3; i++ {
		ik = ensureBuffer(ik, start+i%2)
	}
}
