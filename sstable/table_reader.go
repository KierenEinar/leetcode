package sstable

type blockReader struct {
	data               []byte
	restartPointOffset int
	restartPointNums   int
}

//func (br *blockReader) SeekRestartPoint(key InternalKey) (int, error) {
//
//	n := sort.Search(br.restartPointNums, func(i int) bool {
//
//		restartPoint := br.data[i]
//
//
//
//	})
//
//}
