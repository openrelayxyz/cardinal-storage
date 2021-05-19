package storage

import (
  "math/big"
  "github.com/openrelayxyz/cardinal-types"
)

type KeyValue struct {
  Key []byte
  Value []byte
}

type Storage interface {
  // View returns a transaction for interfacing with the layer indicated by the
  // specified hash.
  View(types.Hash, func(Transaction) error)
  // AddBlock adds a new block to the storage engine. An error will be returned
  // if the parent specified does not exist. If `weight` exceeds the weight of
  // any previously handled block, this block will be returned in future calls
  // to `LatestHash()`. `blockData` and `stateUpdates` will be tracked against
  // this block and eventually persisted. The `resumption` byte string will be
  // provided by the information source, so that backups recovering from this
  // storage engine can determine where to resume from.
  AddBlock(hash, parentHash types.Hash, blockData []KeyValue, number uint64, weight big.Int, destructs [][]byte, stateUpdates []KeyValue, resumption []byte)

  // LatestHash returns the block with the highest weight added through AddBlock
  LatestHash() types.Hash

  // NumberToHash returns the hash of the block with the specified number that
  // is an ancestor of the block at `LatestHash()`
  NumberToHash(uint64) (types.Hash, error)
}

type Transaction interface {
  GetState([]byte) ([]byte, error)
  GetBlockData(types.Hash, []byte) ([]byte, error)
}
