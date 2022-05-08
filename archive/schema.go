package archive

import (
	"encoding/binary"
	"github.com/openrelayxyz/cardinal-types"
)

var (
	HashToNumPrefix      = []byte("h")
	NumToHashPrefix      = []byte("n")
	NumberToWeightPrefix = []byte("w")
	RangePrefix          = []byte("r")
	DataPrefix           = []byte("v")
	ResumptionDataPrefix = []byte("e")
	RollbackPrefix       = []byte("u") // u for undo
	LatestBlockHashKey   = []byte("LatestBlockHash")
	LatestBlockWeightKey = []byte("LatestBlockWeight")
	MemoryPersistenceKey = []byte("MemoryPersistence")
)

func NumberToWeightKey(n uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, n)
	return append(NumberToWeightPrefix, data...)
}
func NumberToResumptionKey(n uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, n)
	return append(ResumptionDataPrefix, data...)
}

func HashToNumKey(h types.Hash) []byte {
	return append(HashToNumPrefix, h[:]...)
}

func NumToHashKey(n uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, n)
	return append(NumToHashPrefix, data...)
}

func RangeKey(key []byte) []byte {
	return append(RangePrefix, key...)
}

func DataKey(key []byte, index uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, index)
	return append(append(DataPrefix, key...), data...)
}

func RollbackKey(n uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, n)
	return append(RollbackPrefix, data...)
}
