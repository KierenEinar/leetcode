package sstable

import "C"
import (
	"container/list"
	"encoding/binary"
	"sort"
	"sync"
)

type VersionSet struct {
	versions    *list.List
	current     *Version
	compactPtrs [kLevelNum]compactPtr
	cmp         *iComparer

	nextFileNum    uint64
	stJournalNum   uint64
	stSeqNum       Sequence // current memtable start seq num
	manifestFd     Fd
	manifestWriter *JournalWriter // lazy init

	storage Storage
}

type Version struct {
	*BasicReleaser
	levels [kLevelNum]tFiles

	// compaction
	cScore float64
	cLevel int
}

type vBuilder struct {
	session  *VersionSet
	base     *Version
	inserted [kLevelNum]*tFileSortedSet
	deleted  [kLevelNum]*uintSortedSet
}

func newBuilder(session *VersionSet, base *Version) *vBuilder {
	builder := &vBuilder{
		session: session,
		base:    base,
	}
	for i := 0; i < kLevelNum; i++ {
		builder.inserted[i] = newTFileSortedSet()
	}
	for i := 0; i < kLevelNum; i++ {
		builder.deleted[i] = newUintSortedSet()
	}
	return builder
}

func (builder *vBuilder) apply(edit VersionEdit) {
	for _, cPtr := range edit.compactPtrs {
		builder.session.compactPtrs[cPtr.level] = cPtr
	}
	for _, delTable := range edit.delTables {
		level, number := delTable.level, delTable.number
		builder.deleted[level].add(number)
	}
	for _, addTable := range edit.addedTables {
		level, number := addTable.level, addTable.number
		builder.deleted[level].remove(number)
		builder.inserted[level].add(addTable)
	}
}

func (builder *vBuilder) saveTo(v *Version) {

	for level := 0; level < kLevelNum; level++ {
		baseFile := v.levels[level]
		beginPos := 0
		iter := builder.inserted[level].NewIterator()
		v.levels[level] = make(tFiles, 0, len(baseFile)+builder.inserted[level].size) // reverse pre alloc capacity
		for iter.Next() {
			addTable, ok := iter.Value().(tFile)
			if !ok {
				panic("vBuilder iter convert value to tFile failed...")
			}
			pos := upperBound(baseFile, level, iter, builder.session.cmp)
			for i := beginPos; i < pos; i++ {
				builder.maybeAddFile(v, baseFile[i], level)
			}
			builder.maybeAddFile(v, addTable, level)
			beginPos = pos
		}

		for i := beginPos; i < len(baseFile); i++ {
			builder.maybeAddFile(v, baseFile[i], level)
		}
	}

}

func upperBound(s tFiles, level int, iter *BTreeIter, cmp BasicComparer) int {

	ok, ikey, fileNum := decodeBinaryToTFile(iter.Key())
	if !ok {
		panic("leveldb decodeBinaryToTFile failed")
	}

	if level == 0 {
		idx := sort.Search(len(s), func(i int) bool {
			return s[i].fd.Num > fileNum
		})
		return idx
	}
	idx := sort.Search(len(s), func(i int) bool {
		return cmp.Compare(s[i].iMax, ikey) > 0
	})
	return idx
}

func (builder *vBuilder) maybeAddFile(v *Version, file tFile, level int) {

	if builder.deleted[level].contains(file.fd.Num) {
		return
	}

	files := v.levels[level]
	cmp := builder.session.cmp
	if level > 0 && len(files) > 0 {
		assert(cmp.Compare(files[len(files)-1].iMax, file.iMin) < 0)
	}

	v.levels[level] = append(v.levels[level], file)
}

type uintSortedSet struct {
	*anySortedSet
}

func newUintSortedSet() *uintSortedSet {
	uSet := &uintSortedSet{
		anySortedSet: &anySortedSet{
			BTree:                 InitBTree(3, &uint64Comparer{}),
			anySortedSetEncodeKey: encodeUint64ToBinary,
		},
	}
	return uSet
}

func encodeUint64ToBinary(item interface{}) (bool, []byte) {

	num, ok := item.(uint64)
	if !ok {
		return false, nil
	}

	switch {
	case num < uint64(1<<16)-1:
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(num))
		return true, buf
	case num < uint64(1<<32)-1:
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(num))
		return true, buf
	default:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, num)
		return true, buf
	}
}

func decodeBinaryToUint64(b []byte) uint64 {

	size := len(b)
	var value uint64
	switch {
	case size == 2:
		value = uint64(binary.LittleEndian.Uint16(b))
	case size == 4:
		value = uint64(binary.LittleEndian.Uint32(b))
	case size == 8:
		value = binary.LittleEndian.Uint64(b)
	default:
		panic("unsupport type decode to uint64")
	}
	return value
}

type uint64Comparer struct{}

func (uc uint64Comparer) Compare(a, b []byte) int {
	uinta, uintb := decodeBinaryToUint64(a), decodeBinaryToUint64(b)
	if uinta < uintb {
		return -1
	} else if uinta == uintb {
		return 0
	} else {
		return 1
	}
}

type tFileSortedSet struct {
	*anySortedSet
}

func newTFileSortedSet() *tFileSortedSet {
	tSet := &tFileSortedSet{
		anySortedSet: &anySortedSet{
			BTree:                 InitBTree(3, &tFileComparer{}),
			anySortedSetEncodeKey: encodeTFileToBinary,
		},
	}
	return tSet
}

type tFileComparer struct {
	*iComparer
}

func (tc *tFileComparer) Compare(a, b []byte) int {

	ia := a[:len(a)-8]
	ib := a[:len(b)-8]
	r := tc.iComparer.Compare(ia, ib)
	if r != 0 {
		return r
	}

	if aNum, bNum := binary.LittleEndian.Uint64(a[len(a)-8:]), binary.LittleEndian.Uint64(b[len(b)-8:]); aNum < bNum {
		return -1
	}
	return 1
}

func encodeTFileToBinary(item interface{}) (bool, []byte) {
	tFile, ok := item.(tFile)
	if !ok {
		return false, nil
	}
	fileNum := make([]byte, 8)
	binary.LittleEndian.PutUint64(fileNum, tFile.fd.Num)
	key := append(tFile.iMax, fileNum...)
	return true, key
}

func decodeBinaryToTFile(b []byte) (bool, InternalKey, uint64) {
	if len(b) < 16 {
		return false, nil, 0
	}
	fileNumBuf := b[len(b)-8:]
	fileNum := binary.LittleEndian.Uint64(fileNumBuf)
	return true, InternalKey(b[:len(b)-8]), fileNum
}

type anySortedSet struct {
	*BTree
	anySortedSetEncodeKey
	addValue bool
	size     int
}

type anySortedSetEncodeKey func(item interface{}) (bool, []byte)

func (set *anySortedSet) add(item interface{}) bool {
	ok, key := set.anySortedSetEncodeKey(item)
	if !ok {
		panic("anySortedSet add item encode failed, please check...")
	}
	if !set.Has(key) {
		if set.addValue {
			set.Insert(key, item)
		} else {
			set.Insert(key, nil)
		}
		set.size++
		return true
	}
	return false
}

func (set *anySortedSet) remove(item interface{}) bool {
	ok, key := set.anySortedSetEncodeKey(item)
	if !ok {
		panic("anySortedSet remove item encode failed, please check...")
	}
	ok = set.Remove(key)
	if ok {
		set.size--
	}
	return ok
}

func (set *anySortedSet) contains(item interface{}) bool {
	ok, key := set.anySortedSetEncodeKey(item)
	if !ok {
		panic("anySortedSet contains item encode failed, please check...")
	}
	return set.Has(key)
}

// LogAndApply apply a new version and record change into manifest file
// noted: thread not safe
// caller should hold a mutex
func (versionSet *VersionSet) logAndApply(edit *VersionEdit, mutex *sync.RWMutex) error {

	assertMutexHeld(mutex)

	/**
	case 1: compactMemtable
		edit.setReq(db.frozenSeqNum)
		edit.setJournalNum(db.journal.Fd)
		edit.addTable(xx)

	case 2:
		table compaction:
			edit.addTable(xx)
			edit.delTable(xx)

	conclusion: all compaction need to be serialize execute.
	but minor compaction, only affect level 0 and is add file. major compaction not affect minor compaction.
	so we can let mem compact and table compact concurrently execute. when install new version, we need a lock
	to protect.
	*/
	if edit.hasRec(kJournalNum) {
		assert(edit.journalNum >= versionSet.stJournalNum)
		assert(edit.journalNum < versionSet.nextFileNum)
	} else {
		edit.journalNum = versionSet.stJournalNum
	}

	if edit.hasRec(kSeqNum) {
		assert(edit.lastSeq >= versionSet.stSeqNum)
	} else {
		edit.lastSeq = versionSet.stSeqNum
	}

	edit.setNextFile(versionSet.nextFileNum)

	// apply new version
	v := &Version{}
	builder := newBuilder(versionSet, versionSet.current)
	builder.apply(*edit)
	builder.saveTo(v)
	finalize(v)

	mutex.Unlock() // cause compaction is run in single thread, so we can avoid expensive write syscall

	var (
		writer         Writer
		storage        = versionSet.storage
		manifestWriter = versionSet.manifestWriter
		manifestFd     = versionSet.manifestFd
		newManifest    bool
		err            error
	)

	if manifestWriter == nil {
		newManifest = true
	}

	if manifestWriter != nil && manifestWriter.size() >= kManifestSizeThreshold {
		newManifest = true
		manifestFd = Fd{
			FileType: Journal,
			Num:      versionSet.allocFileNum(),
		}
	}

	if newManifest {
		writer, err = storage.Create(manifestFd)
		if err == nil {
			manifestWriter = NewJournalWriter(writer)
			err = versionSet.writeSnapShot(manifestWriter) // write current version snapshot into manifest
		}
	}

	if err == nil {
		edit.EncodeTo(manifestWriter)
		err = edit.err
	}

	if err == nil {
		err = storage.SetCurrent(manifestFd)
		if err == nil {
			versionSet.manifestFd = manifestFd
			versionSet.manifestWriter = manifestWriter
		}
	}

	mutex.Lock()

	if err == nil {
		versionSet.appendVersion(v)
		versionSet.stSeqNum = edit.lastSeq
		versionSet.stJournalNum = edit.journalNum
	} else {
		if newManifest {
			err = storage.Remove(manifestFd)
		}
	}

	return err
}

// required: mutex held
// noted: thread not safe
func (versionSet *VersionSet) appendVersion(v *Version) {
	versionSet.versions.PushFront(v)
	old := versionSet.current
	old.UnRef()
	versionSet.current = v
	versionSet.current.Ref()
}

func finalize(v *Version) {

	var (
		bestLevel int
		bestScore float64
	)

	for level := 0; level < len(v.levels); level++ {
		if level == 0 {
			length := len(v.levels[level])
			bestScore = float64(length) / kLevel0StopWriteTrigger
			bestLevel = 0
		} else {
			totalSize := uint64(v.levels[level].size())
			score := float64(totalSize / maxBytesForLevel(level))
			if score > bestScore {
				bestScore = score
				bestLevel = level
			}
		}
	}

	v.cScore = bestScore
	v.cLevel = bestLevel
}

func (versionSet *VersionSet) levelFilesNum(level int) int {
	c := versionSet.current
	c.Ref()
	defer c.UnRef()
	return len(c.levels[level])
}
