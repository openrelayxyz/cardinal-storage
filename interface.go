package storage

import (
  "fmt"
  "math/big"
  "time"
  "github.com/openrelayxyz/cardinal-types"
)

type KeyValue struct {
  Key []byte   `avro:"key"`
  Value []byte `avro:"value"`
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

  // NumberToHash will convert a number to a hash within the range of blocks
  // that can be queried by this Storage interface
  NumberToHash(uint64) (types.Hash, error)

  // Roll back the storage engine to the specified block number in its history
  Rollback(uint64) error

  // LatestBlock returns the hash, height, weight, and resumption token of the
  // latest block
  LatestBlock() (types.Hash, uint64, *big.Int, []byte)

  // Close cleanly shuts down the storage interface.
  Close() error

  // Vacuum frees space in the database. `rollback` indicates the number of
  // deltas to retain to support rollbacks, while gcTime is the an approximate
  // amount of time to spend on database level compaction.
  Vacuum(rollback uint64, gcTime time.Duration)
}

type Initializer interface {
	SetBlockData(hash, parentHash types.Hash, number uint64, weight *big.Int)
	AddData(key, value []byte)
	Close()
}

type Transaction interface {
  // Get returns the data stored at a given key.
  Get([]byte) ([]byte, error)
  // ZeroCopyGet invokes the provided closure with the data stored at the given
  // key. The data should not be modified, and should not be accessed outside
  // the closure without being copied.
  ZeroCopyGet([]byte, func([]byte) error) error
  // NumberToHash returns a block hash given a block number. This may or may
  // not be able to go back to the genesis block, but likely goes back further
  // than Storage.NumberToHash
  NumberToHash(uint64) (types.Hash)
  // HashToNumber returns the block number corresponding to the given hash.
  // This may or may not be able to go back to the genesis block.
  HashToNumber(types.Hash) (uint64)
}


var EmptyHash = types.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
