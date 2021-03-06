package current

import (
  "math/big"
  "encoding/binary"
  "github.com/openrelayxyz/cardinal-storage"
  dbpkg "github.com/openrelayxyz/cardinal-storage/db"
  "github.com/openrelayxyz/cardinal-types"
  log "github.com/inconshreveable/log15"
)

type Initializer struct {
  db dbpkg.Database
  done chan struct{}
  kv chan storage.KeyValue
}

func NewInitializer(db dbpkg.Database) *Initializer {
  init := &Initializer{
    db: db,
    done: make(chan struct{}),
    kv: make(chan storage.KeyValue),
  }
  go func() {
    done := false
    for !done {
      if err := db.Update(func(tr dbpkg.Transaction) error {
        counter := 0
        for counter < 1000 {
          select {
          case kv, ok := <-init.kv:
            if !ok {
              done = true
              return nil
            }
            if err := tr.Put(kv.Key, kv.Value); err != nil {
              log.Crit("Error putting key", "err", err)
              return err
            }
            counter++
          }
        }
        return nil
      }); err != nil { panic(err.Error()) }
    }
    init.done <- struct{}{}
  }()
  return init
}

func (init *Initializer) Close() {
  close(init.kv)
  <-init.done
  init.db.View(func(tr dbpkg.Transaction) error {
    if _, err := tr.Get(LatestBlockHashKey) ; err != nil {
      log.Crit("Could not retrieve LatestBlockHash on shutdown")
    }
    return nil
  })
}

func (init *Initializer) SetBlockData(hash, parentHash types.Hash, number uint64, weight *big.Int) {
  numberBytes := make([]byte, 8)
  binary.BigEndian.PutUint64(numberBytes, number)
  parentNumberBytes := make([]byte, 8)
  binary.BigEndian.PutUint64(parentNumberBytes, number - 1)
  init.kv <- storage.KeyValue{Key: HashToNumKey(hash), Value: numberBytes}
  init.kv <- storage.KeyValue{Key: HashToNumKey(parentHash), Value: parentNumberBytes}
  init.kv <- storage.KeyValue{Key: NumToHashKey(number), Value: hash[:]}
  init.kv <- storage.KeyValue{Key: NumToHashKey(number - 1), Value: parentHash[:]}
  init.kv <- storage.KeyValue{Key: LatestBlockHashKey, Value: hash.Bytes()}
  init.kv <- storage.KeyValue{Key: LatestBlockWeightKey, Value: weight.Bytes()}
}

func (init *Initializer) AddData(key, value []byte) {
  init.kv <- storage.KeyValue{Key: DataKey(key), Value: value}
}
