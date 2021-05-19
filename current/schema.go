package current

import (
  "encoding/binary"
  "github.com/openrelayxyz/cardinal-types"
)


var (
  HashToNumPrefix = []byte("h")
  NumToHashPrefix = []byte("n")
  BlockDataPrefix = []byte("d")
  StateDataPrefix = []byte("s")
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

func BlockDataKey(h types.Hash, key []byte) []byte {
  return append(append(BlockDataPrefix, h[:]...), key...)
}

func StateDataKey(key []byte) []byte {
  return append(StateDataPrefix, key...)
}
