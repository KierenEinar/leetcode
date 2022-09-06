package bplustree

import (
	"testing"
)

func TestBPlusTree_Insert(t *testing.T) {

	tree := NewPlusTree(3)
	for i := uint8(0); i < 20; i++ {
		tree.Insert([]byte{i}, []byte{i})
	}

	tree.BFS()

}
