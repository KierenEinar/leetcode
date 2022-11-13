package sstable

import (
	"container/list"
	"sync"
	"sync/atomic"
)

type Sequence uint64

type DB struct {
	rwMutex    sync.RWMutex
	VersionSet *VersionSet

	journalWriter *JournalWriter

	closed uint32

	// these state are protect by mutex
	seqNum    Sequence
	journalFd Fd

	frozenSeq       Sequence
	frozenJournalFd Fd

	mem  *MemTable
	immu *MemTable

	backgroundWorkFinishedSignal *sync.Cond

	bgErr error

	scratchBatch *WriteBatch

	writers *list.List
}

func (db *DB) write(batch *WriteBatch) error {

	if atomic.LoadUint32(&db.closed) == 1 {
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
		db.mergeWriteBatch(batch, &lastWriter)
		db.scratchBatch.SetSequence(lastSequence)
		lastSequence += Sequence(db.scratchBatch.Len())
		mem := db.mem
		mem.Ref()
		db.rwMutex.Unlock()
		// expensive syscall need to unlock !!!
		_, err = db.journalWriter.Write(db.scratchBatch.Contents())
		db.rwMutex.Lock()
		if err == nil {
			db.writeMem(mem, db.scratchBatch)
			db.scratchBatch.Reset()
			db.seqNum = lastSequence
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
	return nil
}

func (db *DB) mergeWriteBatch(batch *WriteBatch, lastWriter **writer) {

	assertMutexHeld(&db.rwMutex)

	assert(db.writers.Len() > 0)

	maxSize := 1 << 20       // 1m
	if batch.Len() < 1<<17 { // limit the growth while in small write
		maxSize = 1 << 17
	}

	maxSize = batch.Size() + maxSize
	w := db.writers.Front()

	for {
		wr := w.Value.(*writer)
		if db.scratchBatch.Size()+wr.batch.Size() > maxSize {
			break
		}
		lastWriter = &wr
		db.scratchBatch.append(wr.batch)
		w = w.Next()
		if w == nil {
			break
		}
	}

}
