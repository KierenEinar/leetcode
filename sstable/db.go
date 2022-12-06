package sstable

import (
	"container/list"
	"encoding/binary"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Sequence uint64

type DB struct {
	rwMutex    sync.RWMutex
	VersionSet *VersionSet

	journalWriter *JournalWriter

	shutdown uint32

	// these state are protect by mutex
	seqNum    Sequence
	journalFd Fd

	frozenSeq       Sequence
	frozenJournalFd Fd

	mem *MemDB
	imm *MemDB

	backgroundWorkFinishedSignal *sync.Cond

	backgroundCompactionScheduled bool

	bgErr error

	scratchBatch *WriteBatch

	writers *list.List

	// atomic state
	hasImm uint32

	tableOperation *tableOperation
}

func (db *DB) get(key []byte) ([]byte, error) {

}

func (db *DB) write(batch *WriteBatch) error {

	if atomic.LoadUint32(&db.shutdown) == 1 {
		return ErrClosed
	}

	if batch.Len() == 0 {
		return nil
	}

	w := newWriter(batch, &db.rwMutex)
	db.rwMutex.Lock()
	db.writers.PushBack(w)

	header := db.writers.Front().Value.(*writer)
	for w != header {
		w.cv.Wait()
	}

	if w.done {
		return w.err
	}

	// may temporary unlock and lock mutex
	err := db.makeRoomForWrite()
	lastWriter := w

	lastSequence := db.seqNum

	if err == nil {
		newWriteBatch := db.mergeWriteBatch(&lastWriter) // write into scratchbatch
		db.scratchBatch.SetSequence(lastSequence + 1)
		lastSequence += Sequence(db.scratchBatch.Len())
		mem := db.mem
		mem.Ref()
		db.rwMutex.Unlock()
		// expensive syscall need to unlock !!!
		_, syncErr := db.journalWriter.Write(newWriteBatch.Contents())
		if syncErr == nil {
			err = db.writeMem(mem, newWriteBatch)
		}

		db.rwMutex.Lock()
		db.seqNum = lastSequence

		if syncErr != nil {
			db.recordBackgroundError(syncErr)
		}

		if newWriteBatch == db.scratchBatch {
			db.scratchBatch.Reset()
		}

		ready := db.writers.Front()
		for {
			readyW := ready.Value.(*writer)
			if readyW != lastWriter {
				readyW.done = true
				readyW.err = err
				readyW.cv.Signal()
			}
			db.writers.Remove(ready)
			if readyW == lastWriter {
				break
			}
			ready = ready.Next()
		}

		if ready.Next() != nil {
			readyW := ready.Value.(*writer)
			readyW.cv.Signal()
		}

	}

	db.rwMutex.Unlock()

	return err
}

func (db *DB) writeMem(mem *MemDB, batch *WriteBatch) error {

	seq := batch.seq
	idx := 0

	reqLen := len(batch.rep)

	for i := 0; i < batch.count; i++ {
		c := batch.rep[idx]
		idx += 1
		assert(idx < reqLen)
		kLen, n := binary.Uvarint(batch.rep[idx:])
		idx += n

		assert(idx < reqLen)
		var (
			key []byte
			val []byte
		)

		switch c {
		case kTypeValue:
			vLen, n := binary.Uvarint(batch.rep[idx:])
			idx += n
			assert(idx < reqLen)

			key = batch.rep[idx : idx+int(kLen)]
			idx += int(kLen)

			val = batch.rep[idx : idx+int(vLen)]
			idx += int(vLen)
			assert(idx < reqLen)

			err := mem.Put(key, seq+Sequence(i), val)
			if err != nil {
				return err
			}

		case kTypeDel:
			key = batch.rep[idx : idx+int(kLen)]
			idx += int(kLen)
			assert(idx < reqLen)

			err := mem.Del(key, seq+Sequence(i))
			if err != nil {
				return err
			}

		default:
			panic("invalid key type support")
		}

	}

	return nil

}

func (db *DB) makeRoomForWrite() error {

	assertMutexHeld(&db.rwMutex)
	allowDelay := true

	for {
		if db.bgErr != nil {
			return db.bgErr
		} else if allowDelay && db.VersionSet.levelFilesNum(0) >= kLevel0SlowDownTrigger {
			allowDelay = false
			db.rwMutex.Unlock()
			time.Sleep(time.Microsecond * 1000)
			db.rwMutex.Lock()
		} else if db.mem.ApproximateSize() <= kMemTableWriteBufferSize {
			break
		} else if db.imm != nil { // wait background compaction compact imm table
			db.backgroundWorkFinishedSignal.Wait()
		} else if db.VersionSet.levelFilesNum(0) >= kLevel0StopWriteTrigger {
			db.backgroundWorkFinishedSignal.Wait()
		} else {

			journalFd := Fd{
				FileType: KJournalFile,
				Num:      db.VersionSet.allocFileNum(),
			}
			stor := db.VersionSet.storage
			writer, err := stor.Create(journalFd)
			if err == nil {
				_ = db.journalWriter.Close()
				db.frozenSeq = db.seqNum
				db.frozenJournalFd = db.journalFd
				db.journalFd = journalFd
				db.journalWriter = NewJournalWriter(writer)
				imm := db.imm
				imm.UnRef()
				db.imm = db.mem
				atomic.StoreUint32(&db.hasImm, 1)
				mem := NewMemTable(kMemTableWriteBufferSize, db.VersionSet.cmp)
				mem.Ref()
				db.mem = mem
			} else {
				db.VersionSet.reuseFileNum(journalFd.Num)
				return err
			}
			db.MaybeScheduleCompaction()
		}
	}

	return nil
}

func (db *DB) mergeWriteBatch(lastWriter **writer) *WriteBatch {

	assertMutexHeld(&db.rwMutex)

	assert(db.writers.Len() > 0)

	front := db.writers.Front()
	firstBatch := front.Value.(*writer).batch
	size := firstBatch.Size()

	maxSize := 1 << 20  // 1m
	if size < 128<<10 { // limit the growth while in small write
		maxSize = size + 128<<10
	}

	result := firstBatch
	w := front.Next()
	for w != nil {
		wr := w.Value.(*writer)
		if size+wr.batch.Size() > maxSize {
			break
		}
		if result == firstBatch {
			result = db.scratchBatch
			result.append(firstBatch)
		}
		result.append(wr.batch)
		lastWriter = &wr
		w = w.Next()
	}

	return result

}

func (db *DB) recordBackgroundError(err error) {
	if db.bgErr == nil {
		db.bgErr = err
		db.backgroundWorkFinishedSignal.Broadcast()
	}
}

// MaybeScheduleCompaction required mutex held
func (db *DB) MaybeScheduleCompaction() {
	assertMutexHeld(&db.rwMutex)

	if db.backgroundCompactionScheduled {
		// do nothing
	} else if db.bgErr != nil {
		// do nothing
	} else if atomic.LoadUint32(&db.shutdown) == 1 {
		// do nothing
	} else if atomic.LoadUint32(&db.hasImm) == 0 && !db.VersionSet.needCompaction() {
		// do nothing
	} else {
		go db.backgroundCall()
	}

}

func (db *DB) backgroundCall() {

	db.rwMutex.Lock()

	assert(db.backgroundCompactionScheduled)

	if db.bgErr != nil {
		// do nothing
	} else if atomic.LoadUint32(&db.shutdown) == 1 {
		// do nothing
	} else {
		db.backgroundCompaction()
	}

	db.backgroundCompactionScheduled = false
	db.MaybeScheduleCompaction()
	db.rwMutex.Unlock()

	db.backgroundWorkFinishedSignal.Broadcast()

}

func (db *DB) backgroundCompaction() {
	assertMutexHeld(&db.rwMutex)

	if db.imm != nil {
		db.compactMemTable()
		return
	}

	c := db.VersionSet.pickCompaction1()
	if c == nil {
		return
	} else if len(c.inputs[0]) == 1 && len(c.inputs[1]) == 0 && c.gp.size() <= c.gpOverlappedLimit {

		edit := &VersionEdit{}
		addTable := c.inputs[0][0]
		edit.addNewTable(c.cPtr.level+1, addTable.Size, addTable.fd.Num, addTable.iMin, addTable.iMax)
		err := db.VersionSet.logAndApply(edit, &db.rwMutex)
		if err != nil {
			db.recordBackgroundError(err)
		}
	} else {
		err := db.doCompactionWork(c)
		if err != nil {
			db.recordBackgroundError(err)
		}
		c.releaseInputs()
		err = db.removeObsoleteFiles()
		if err != nil {
			// todo log warn msg
		}

	}

}

func (db *DB) compactMemTable() {

	assertMutexHeld(&db.rwMutex)
	assert(db.imm != nil)

	edit := &VersionEdit{}
	err := db.writeLevel0Table(db.imm, edit)
	if err == nil {
		imm := db.imm
		db.imm = nil
		imm.UnRef()
		atomic.StoreUint32(&db.hasImm, 1)
		edit.setLogNum(db.journalFd.Num)
		edit.setLastSeq(db.frozenSeq)
		err = db.VersionSet.logAndApply(edit, &db.rwMutex)
		if err == nil {
			err = db.removeObsoleteFiles()
		}
	}

	if err != nil {
		db.recordBackgroundError(err)
	}
}

func (db *DB) writeLevel0Table(memDb *MemDB, edit *VersionEdit) (err error) {

	db.rwMutex.Unlock()
	defer db.rwMutex.Lock()

	tWriter, err := db.tableOperation.create()
	if err != nil {
		return err
	}

	iter := memDb.NewIterator()
	defer iter.UnRef()

	for iter.Next() {
		err = tWriter.append(iter.Key(), iter.Value())
		if err != nil {
			db.rwMutex.Lock()
			return err
		}
	}

	tFile, err := tWriter.finish()
	if err == nil {
		edit.addNewTable(0, tFile.Size, tFile.fd.Num, tFile.iMin, tFile.iMax)
	}
	return
}

func Open(dbpath string) (*DB, error) {
	storage, err := OpenPath(dbpath)
	if err != nil {
		return nil, err
	}

	db := &DB{
		VersionSet: &VersionSet{
			cmp:       IComparer,
			storage:   storage,
			versions:  list.New(),
			snapshots: list.New(),
		},
	}

	tableOperation := newTableOperation(storage, db.VersionSet)
	db.VersionSet.tableOperation = tableOperation

	db.rwMutex.Lock()
	defer db.rwMutex.Unlock()

	err = db.recover()
	if err != nil {
		return nil, err
	}

	memDB := NewMemTable(0, db.VersionSet.cmp)
	memDB.Ref()

	db.mem = memDB
	journalFd := Fd{
		FileType: KJournalFile,
		Num:      db.VersionSet.allocFileNum(),
	}
	sequentialWriter, err := storage.Create(journalFd)
	if err != nil {
		return nil, err
	}
	db.journalFd = journalFd
	db.journalWriter = NewJournalWriter(sequentialWriter)

	edit := &VersionEdit{}
	edit.setLastSeq(db.seqNum)
	edit.setLogNum(db.journalFd.Num)
	err = db.VersionSet.logAndApply(edit, &db.rwMutex)
	if err != nil {
		return nil, err
	}

	//todo warn err log
	err = db.removeObsoleteFiles()
	db.MaybeScheduleCompaction()

	return db, nil
}

func (db *DB) recover() error {
	manifestFd, err := db.VersionSet.storage.GetCurrent()
	if err != nil {
		if err != os.ErrNotExist {
			return err
		}
		err = db.newDb()
	} else {
		err = db.VersionSet.recover(manifestFd)
	}

	if err != nil {
		return err
	}

	fds, err := db.VersionSet.storage.List()
	if err != nil {
		return err
	}

	var expectedFiles = make(map[uint64]struct{})
	db.VersionSet.addLiveFiles(expectedFiles)

	logFiles := make([]Fd, 0)

	for _, fd := range fds {
		if fd.FileType == KTableFile {
			delete(expectedFiles, fd.Num)
		} else if fd.FileType == KJournalFile && fd.Num >= db.VersionSet.stJournalNum {
			logFiles = append(logFiles, fd)
		}
	}

	if len(expectedFiles) > 0 {
		err = NewErrCorruption("invalid table file, file not exists")
		return err
	}

	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].Num < logFiles[j].Num
	})

	var edit VersionEdit

	for _, logFile := range logFiles {
		err = db.recoverLogFile(logFile, &edit)
		if err != nil {
			return err
		}
	}

	err = db.VersionSet.logAndApply(&edit, &db.rwMutex)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) newDb() (err error) {
	db.seqNum = 0
	db.journalFd = Fd{
		FileType: KJournalFile,
		Num:      1,
	}

	manifestFd := Fd{
		FileType: KDescriptorFile,
		Num:      2,
	}

	writer, wErr := db.VersionSet.storage.Create(manifestFd)
	if wErr != nil {
		err = wErr
		return
	}

	defer func() {
		if err == nil {
			return
		}
		_ = writer.Close()
		_ = db.VersionSet.storage.Remove(manifestFd)
	}()

	db.journalWriter = NewJournalWriter(writer)

	newDb := &VersionEdit{
		journalNum:   db.journalFd.Num,
		nextFileNum:  3,
		lastSeq:      0,
		comparerName: IComparer.Name(),
	}

	newDb.EncodeTo(db.journalWriter)
	if newDb.err != nil {
		err = newDb.err
		return
	}

	err = db.VersionSet.storage.SetCurrent(manifestFd.Num)

	return

}

func (db *DB) recoverLogFile(fd Fd, edit *VersionEdit) error {

	reader, err := db.VersionSet.storage.Open(fd)
	if err != nil {
		return err
	}
	journalReader := NewJournalReader(reader)
	memDB := NewMemTable(0, db.VersionSet.cmp)
	memDB.Ref()
	defer func() {
		memDB.UnRef()
		journalReader.Close()
		_ = reader.Close()
	}()
	for {
		sequentialReader, err := journalReader.NextChunk()

		if err == io.EOF {
			break
		}
		writeBatch, err := buildBatchGroup(sequentialReader, db.VersionSet.stSeqNum)

		if err != nil {
			return err
		}

		if memDB.ApproximateSize() > kMemTableWriteBufferSize {
			err = db.writeLevel0Table(memDB, edit)
			if err != nil {
				return err
			}
			memDB.UnRef()

			memDB = NewMemTable(0, db.VersionSet.cmp)
			memDB.Ref()
		}

		err = writeBatch.insertInto(memDB)
		if err != nil {
			return err
		}

		db.seqNum += writeBatch.seq + Sequence(writeBatch.count) - 1

		db.VersionSet.markFileUsed(fd.Num)

	}

	if memDB.Size() > 0 {
		err = db.writeLevel0Table(memDB, edit)
		if err != nil {
			return err
		}
	}

	if db.VersionSet.nextFileNum != db.VersionSet.manifestFd.Num {
		db.VersionSet.manifestFd = Fd{
			FileType: KDescriptorFile,
			Num:      db.VersionSet.allocFileNum(),
		}
	}

	return nil

}

// clear the obsolete files
func (db *DB) removeObsoleteFiles() (err error) {

	assertMutexHeld(&db.rwMutex)

	fds, lErr := db.VersionSet.storage.List()
	if lErr != nil {
		err = lErr
		return
	}

	version := db.VersionSet.getCurrent()

	liveTableFileSet := make(map[Fd]struct{})
	for _, levels := range version.levels {
		for _, v := range levels {
			liveTableFileSet[v.fd] = struct{}{}
		}
	}

	fileToClean := make([]Fd, 0)

	for _, fd := range fds {
		var keep bool
		switch fd.FileType {
		case KDescriptorFile:
			keep = fd.Num >= db.VersionSet.manifestFd.Num
		case KJournalFile:
			keep = fd.Num >= db.VersionSet.stJournalNum
		case KTableFile:
			if _, ok := liveTableFileSet[fd]; ok {
				keep = true
			}
		case KCurrentFile, KDBLockFile, KDBTempFile:
			keep = true
		}

		if !keep {
			fileToClean = append(fileToClean, fd)
		}

	}

	db.rwMutex.Unlock()

	for _, fd := range fileToClean {
		rErr := db.VersionSet.storage.Remove(fd)
		if rErr != nil {
			err = rErr
		}

		// todo evict table cache

	}

	db.rwMutex.Lock()
	return
}
