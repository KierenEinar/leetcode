package sstable

/**
session info need to be sync into file storage
1. comparer name
2. last journal num that haven't been read into sstable file
3. nextFileNum
4. lastSeq num that have been sync into sstable file
5. compact ptr
6. del table file
7. add table file
*/
const (
	kRecComparer   = 1
	kRecJournalNum = 2
)

type SessionRecord struct {
}
