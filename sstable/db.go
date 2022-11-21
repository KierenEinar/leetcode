package sstable

import (
	"container/list"
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
		_, err = db.journalWriter.Write(newWriteBatch.Contents())
		if err == nil {
			db.writeMem(mem, newWriteBatch)
			db.rwMutex.Lock()
			if newWriteBatch == db.scratchBatch {
				db.scratchBatch.Reset()
			}
			db.seqNum = lastSequence
		} else {
			db.recordBackgroundError(err)
			return err
		}

		ready := db.writers.Front()
		for {
			readyW := ready.Value.(*writer)
			if readyW.batch == batch {
				err = readyW.err
			}
			db.writers.Remove(ready)
			if readyW == lastWriter {
				break
			}
			readyW.done = true
			readyW.err = err
			readyW.cv.Signal()
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

			db.frozenSeq = db.seqNum
			db.frozenJournalFd = db.journalFd

			journalFd := Fd{
				FileType: Journal,
				Num:      db.VersionSet.allocFileNum(),
			}
			stor := db.VersionSet.storage
			writer, err := stor.Create(journalFd)
			if err == nil {
				_ = db.journalWriter.Close()
				db.journalFd = journalFd
				db.journalWriter = NewJournalWriter(writer)
				immu := db.imm
				immu.UnRef()
				db.imm = db.mem
				atomic.StoreUint32(&db.hasImm, 1)
				db.mem = NewMemTable()
				db.mem.Ref()
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

}

func (db *DB) compactMemTable() {

	assertMutexHeld(&db.rwMutex)
	assert(db.imm != nil)

	edit := &VersionEdit{}
	base := db.VersionSet.current
	base.Ref()
	err := db.writeLevel0Table(db.imm, edit, base)
	base.UnRef()

}

func (db *DB) writeLevel0Table(mem *MemDB, edit *VersionEdit, base *Version) error {

}
