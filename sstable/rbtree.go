package sstable

/**
llrbtree describe:
	llrbtree is a more easy to implement, which based on 2-3 binary tree theory.




							/----/
							|	 |
							/----/
						//





**/

type LLRBTree struct {
	root *LLRBTreeNode
}

type LLRBTreeNode struct {
	isRed bool
	key   []byte
	value []byte
}

func (node *LLRBTreeNode) IsRed() bool {
	return node.isRed
}
