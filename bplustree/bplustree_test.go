package bplustree

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
	"unsafe"
)

func TestBPlusTree_Insert(t *testing.T) {

	tree := NewPlusTree(3)
	for i := uint8(0); i < 255; i++ {
		tree.Insert([]byte{i}, []byte{i})
	}

	tree.BFS()

	for i := uint8(0); i < 255; i++ {
		v := tree.Get([]byte{i})
		t.Logf("key=%d,value=%d", i, v)
	}

	//for i := 14; i >= 0; i-- {
	//	removed := tree.Remove([]byte{uint8(i)})
	//	t.Logf("k=%d, removed=%v", i, removed)
	//	tree.BFS()
	//}

	for i := 0; i < 255; i++ {
		removed := tree.Remove([]byte{uint8(i)})
		t.Logf("k=%d, removed=%v", i, removed)
		tree.BFS()
	}

	//
	//removed = tree.Remove([]byte{253})
	//t.Logf("removed=%v", removed)
	//
	//removed = tree.Remove([]byte{252})
	//t.Logf("removed=%v", removed)
	//
	//removed = tree.Remove([]byte{251})
	//t.Logf("removed=%v", removed)

	//tree.BFS()

}

func randC() []byte {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	s := fmt.Sprintf("%x", r.Intn(1e8))
	b := *(*[]byte)(unsafe.Pointer(&s))
	return b
}
