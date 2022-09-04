package btree

import (
	"testing"
)

func TestBtree_InsertAndGet(t *testing.T) {

	// new tree with 3 degree
	tree := NewTree(3)
	for i := uint8(0); i < 100; i++ {
		tree.Insert([]byte{i}, []byte{i})
	}

	/**
	tree look like this
				    10
		3   6  7    	   13     16
	1 2  4 5    8 9   11 12  14 15  17 18 19 20
	*/

	//for i:=0;i<100;i++ {
	//	v := tree.Get(src)
	//	fmt.Printf("%#v", v)
	//}

}
