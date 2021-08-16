package main

import (
  "bufio"
  "encoding/json"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/openrelayxyz/cardinal-types/hexutil"
  "github.com/openrelayxyz/cardinal-storage/current"
  "github.com/openrelayxyz/cardinal-storage/db/badgerdb"
  "os"
  log "github.com/inconshreveable/log15"
)

type Record struct{
  Key        string        `json:"key"`
  Value      hexutil.Bytes `json:"value"`
  Hash       types.Hash  `json:"hash"`
  ParentHash types.Hash  `json:"parentHash"`
  Number     uint64      `json:"number"`
  Weight     hexutil.Big `json:"weight"`
}

func main() {
  db, err := badgerdb.New(os.Args[1])
  if err != nil { panic(err.Error()) }
  init := current.NewInitializer(db)
  reader := bufio.NewReader(os.Stdin)
  record := []byte{}
  for counter := 0 ; err == nil ; counter++ {
    line, isPrefix, err := reader.ReadLine()
    if err != nil { break }
    record = append(record, line...)
    if !isPrefix {
      r := Record{}
      err = json.Unmarshal(record, &r)
      if r.Hash != (types.Hash{}) {
        log.Info("Recording block data", "hash", r.Hash, "num", r.Number)
        init.SetBlockData(r.Hash, r.ParentHash, r.Number, r.Weight.ToInt())
      } else {
        init.AddData([]byte(r.Key), r.Value)
      }
    }
    record = []byte{}
    if counter % 100000 == 0 {
      log.Info("Lines", "count", counter)
    }
  }
  if err != nil { panic(err.Error())}
  init.Close()
  db.Close()
}
