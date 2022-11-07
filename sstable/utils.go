package sstable

import (
	"reflect"
	"sync"
)

const mutexLocked = 1

func assertMutexHeld(mutex *sync.Mutex) bool {
	state := reflect.ValueOf(mutex).FieldByName("state")
	ok := state.Int()&mutexLocked == mutexLocked
	if !ok {
		panic("assertMutexHeld mutex not locked!!!, may be is bug, go check?")
	}
}
