package main

import (
  "encoding/json"
  "encoding/binary"
  "math/big"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/openrelayxyz/cardinal-types/hexutil"
  current "github.com/openrelayxyz/cardinal-storage/current"
  dbpkg "github.com/openrelayxyz/cardinal-storage/db"
  "github.com/openrelayxyz/cardinal-storage/db/badgerdb"
  "os"
)

type Output struct{
  Key string `json:"key"`
  Value hexutil.Bytes `json:"value"`
}

type BlockMetaOutput struct{
  Hash       types.Hash `json:"hash"`
  ParentHash types.Hash `json:"parentHash"`
  Number     uint64      `json:"number"`
  Weight     hexutil.Big `json:"weight"`
}
func main() {
  db, err := badgerdb.New(os.Args[1])
  if err != nil {
    // if db != nil { db.Close() }
    panic(err.Error())
  }
  defer db.Close()
  jsonStream := json.NewEncoder(os.Stdout)
  db.View(func(tr dbpkg.Transaction) error {
    // Initial block metadata
    func() {
      bmo := &BlockMetaOutput{}
      hashBytes, err := tr.Get(current.LatestBlockHashKey)
      if err != nil { return }
      bmo.Hash = types.BytesToHash(hashBytes)
      numBytes, err := tr.Get(current.HashToNumKey(bmo.Hash))
      if err != nil  { return }
      bmo.Number = binary.BigEndian.Uint64(numBytes)
      weightBytes, err := tr.Get(current.LatestBlockWeightKey)
      if err != nil  { return }
      bmo.Weight = hexutil.Big(*new(big.Int).SetBytes(weightBytes))
      parentHashBytes, err := tr.Get(current.NumToHashKey(bmo.Number - 1))
      if err != nil  { return }
      bmo.ParentHash = types.BytesToHash(parentHashBytes)
      jsonStream.Encode(bmo)
    }()

    prefix := []byte("d")
    if len(os.Args) > 2 {
      prefix = append(prefix, []byte(os.Args[2])...)
    }
    iter := tr.Iterator(prefix)
    for iter.Next() {
      jsonStream.Encode(Output{string(iter.Key()[1:]), hexutil.Bytes(iter.Value())})
    }
    if err := iter.Error(); err != nil {
      db.Close()
      panic(err.Error())
    }
    iter.Close()
    return nil
  })
}
