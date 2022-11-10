package sstable

const kMaxSequenceNum = (uint64(1) << 56) - 1
const kMaxNum = kMaxSequenceNum | uint64(keyTypeValue)

var magicByte = []byte("\x57\xfb\x80\x8b\x24\x75\x47\xdb")

const blockTailLen = 5
const tableFooterLen = 48
const journalBlockHeaderLen = 7
const kJournalBlockSize = 1 << 15
const kWritableBufferSize = 1 << 16
const kLevelNum = 7
const kLevel0SlowDownTrigger = 8
const kLevel0StopWriteTrigger = 12
const kManifestSizeThreshold = 1 << 26 // 64m

const kLevel1SizeThreshold = 10 * (1 << 20) //10m

func maxBytesForLevel(level int) uint64 {
	result := uint64(kLevel1SizeThreshold)
	for level > 1 {
		result *= 10
		level--
	}
	return result
}