package current

import (
  "encoding/binary"
  "github.com/openrelayxyz/cardinal-types"
)


var (
  HashToNumPrefix = []byte("h")
  NumToHashPrefix = []byte("n")
  DataPrefix = []byte("d")
  ResumptionDataKey = []byte("Resumption")
  LatestBlockHashKey = []byte("LatestBlockHash")
  LatestBlockWeightKey = []byte("LatestBlockWeight")
)


func HashToNumKey(h types.Hash) []byte {
  return append(HashToNumPrefix, h[:]...)
}

func NumToHashKey(n uint64) []byte {
  data := make([]byte, 8)
  binary.BigEndian.PutUint64(data, n)
  return append(NumToHashPrefix, data...)
}

func DataKey(key []byte) []byte {
  return append(DataPrefix, key...)
}
