package storage

import (
  "fmt"
  "math/big"
  "github.com/openrelayxyz/cardinal-types"
)

type KeyValue struct {
  Key []byte
  Value []byte
}

func (kv KeyValue) String() string {
  return fmt.Sprintf(`{"%x": "%x"}`, kv.Key, kv.Value)
}

type Storage interface {
  // View returns a transaction for interfacing with the layer indicated by the
  // specified hash.
  View(types.Hash, func(Transaction) error) error
  // AddBlock adds a new block to the storage engine. An error will be returned
  // if the parent specified does not exist. If `weight` exceeds the weight of
  // any previously handled block, this block will be returned in future calls
  // to `LatestHash()`. `blockData` and `stateUpdates` will be tracked against
  // this block and eventually persisted. The `resumption` byte string will be
  // provided by the information source, so that backups recovering from this
  // storage engine can determine where to resume from.
  AddBlock(hash, parentHash types.Hash, number uint64, weight *big.Int, updates []KeyValue, deletes [][]byte, resumption []byte) error

  // LatestHash returns the block with the highest weight added through AddBlock
  LatestHash() types.Hash

  // NumberToHash returns the hash of the block with the specified number that
  // is an ancestor of the block at `LatestHash()`
  NumberToHash(uint64) (types.Hash, error)
}

type Transaction interface {
  Get([]byte) ([]byte, error)
  ZeroCopyGet([]byte, func([]byte) error) error
}


var EmptyHash = types.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
