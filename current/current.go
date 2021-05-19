package current

import (
  "bytes"
  "encoding/binary"
  "fmt"
  "math/big"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/openrelayxyz/cardinal-storage"
  "github.com/openrelayxyz/cardinal-storage/db"
)

type currentStorage struct {
  db db.Database
  layers map[types.Hash]layer
  latestHash types.Hash
  reorgDepth uint64
}

// View returns a transaction for interfacing with the layer indicated by the
// specified hash
func (s *currentStorage) View(hash types.Hash, fn func(storage.Transaction) error) error {
  layer, ok := s.layers[hash]
  if !ok {
    return fmt.Errorf("Layer unavailable for hash %#x", hash)
  }
  return s.db.View(func(tr db.Transaction) error {
    ctr := &currentTransaction{
      transaction: tr,
      layer: layer,
    }
    return fn(ctr)
  })
}

// AddBlock adds a new block to the storage engine. An error will be returned
// if the parent specified does not exist. If `weight` exceeds the weight of
// any previously handled block, this block will be returned in future calls
// to `LatestHash()`. `blockData` and `stateUpdates` will be tracked against
// this block and eventually persisted. The `resumption` byte string will be
// provided by the information source, so that backups recovering from this
// storage engine can determine where to resume from.
func (s *currentStorage) AddBlock(hash, parentHash types.Hash, blockData []storage.KeyValue, number uint64, weight *big.Int, destructs [][]byte, stateUpdates []storage.KeyValue, resumption []byte) error {
  parentLayer, ok := s.layers[parentHash]
  if !ok {
    return fmt.Errorf("Parent block %#x missing", parentHash)
  }
  newLayer := &memoryLayer{
    hash: hash,
    parent: parentLayer,
    num: number,
    blockWeight: weight,
    blockDataMap: make(map[string][]byte),
    stateUpdatesMap: make(map[string][]byte),
    destructsMap: make(map[string]bool),
    destructs: destructs,
    resumption: resumption,
  }
  s.layers[hash] = newLayer
  for _, kv := range blockData {
    newLayer.blockDataMap[string(kv.Key)] = kv.Value
  }
  for _, kv := range stateUpdates {
    newLayer.stateUpdatesMap[string(kv.Key)] = kv.Value
  }
  for _, k := range destructs {
    newLayer.destructsMap[string(k)] = true

  }
  if weight.Cmp(newLayer.weight()) > 0 {
    // TODO: Maybe need to use an atomic value for s.latestHash
    s.latestHash = hash
  }
  // TODO: We need to consolidate the parent layers up into the disk layer
  parentLayer.consolidate(
    newLayer.hash,
    newLayer.blockWeight,
    newLayer.blockData,
    newLayer.num,
    newLayer.destructs,
    newLayer.stateUpdates,
    newLayer.resumption,
    newLayer.reorgDepth <= 0,
  )
  return nil
}

// LatestHash returns the block with the highest weight added through AddBlock
func (s *currentStorage) LatestHash() types.Hash {
  return s.latestHash
}

// NumberToHash returns the hash of the block with the specified number that
// is an ancestor of the block at `LatestHash()`
func (s *currentStorage) NumberToHash(num uint64) (types.Hash, error) {
  layer := s.layers[s.latestHash]
  if layer.number() < num {
    fmt.Errorf("Requested hash of future block %v", num)
  }
  for layer != nil && num > layer.number() {
    layer = layer.parentLayer()
  }
  if layer == nil {
    return types.Hash{}, fmt.Errorf("Number %v not availale in history", num)
  }
  if layer.number() != num {
    return types.Hash{}, fmt.Errorf("Unknown error occurred finding block number %v", num)
  }
  return layer.getHash(), nil
}

type currentTransaction struct {
  transaction db.Transaction
  layer layer
}

func (t *currentTransaction) GetState(key []byte) ([]byte, error) {
  return t.layer.getState(key, t.transaction)
}

func (t *currentTransaction) GetBlockData(h types.Hash, key []byte) ([]byte, error) {
  return t.layer.getBlockData(h, key, t.transaction)
}

// type Storage interface {
//   View(types.Hash, func(Transaction) error)
//   AddBlock(hash, parentHash types.Hash, blockData []storage.KeyValue, number uint64, weight big.Int, destructs [][]byte, stateUpdates []storage.KeyValue)
//   LatestHash() types.Hash
//   NumberToHash(uint64) types.Hash
// }
//
// type Transaction interface {
//   GetState([]byte) ([]byte, error)
//   GetBlockData([]byte) ([]byte, error)
// }


type memoryLayer struct {
  parent layer
  num uint64
  hash types.Hash
  blockWeight *big.Int
  blockDataMap map[string][]byte
  stateUpdatesMap map[string][]byte
  destructsMap map[string]bool
  destructs [][]byte
  blockData []storage.KeyValue
  stateUpdates []storage.KeyValue
  resumption []byte
  reorgDepth int64
}

func (l *memoryLayer) consolidate(hash types.Hash, weight *big.Int, blockData []storage.KeyValue, number uint64, destructs [][]byte, stateUpdates []storage.KeyValue, resumption []byte, merge bool) layer {
  l.parent = l.parent.consolidate(l.hash, l.blockWeight, l.blockData, l.num, l.destructs, l.stateUpdates, l.resumption, l.reorgDepth <= 0)
  if l.reorgDepth <= 0 {
    return l.parent
  }
  return l
}
func (l *memoryLayer) number() uint64 {
  return l.num
}
func (l *memoryLayer) getHash() types.Hash {
  return l.hash
}
func (l *memoryLayer) parentLayer() layer {
  return l.parent
}
func (l *memoryLayer) weight() *big.Int {
  return l.blockWeight
}

func (l *memoryLayer) getState(key []byte, tr db.Transaction) ([]byte, error) {
  if l.isDestructed(key) {
    return []byte{}, ErrNotFound
  }
  if val, ok := l.stateUpdatesMap[string(key)]; ok {
    return val, nil
  }
  return l.parent.getState(key, tr)
}

func (l *memoryLayer) isDestructed(key []byte) bool {
  components := bytes.Split(key, []byte("/"))
  for i := 1; i <= len(components); i++ {
    if l.destructsMap[string(bytes.Join(components[:i], []byte("/")))] {
      return true
    }
  }
  return false
}
func (l *memoryLayer) getBlockData(h types.Hash, key []byte, tr db.Transaction) ([]byte, error) {
  if l.hash != h {
    return l.parent.getBlockData(h, key, tr)
  }
  if val, ok := l.blockDataMap[string(key)]; ok {
    return val, nil
  }
  return []byte{}, ErrNotFound
}


// View(types.Hash, func(Transaction) error)
// AddBlock(hash, parentHash types.Hash, blockData []storage.KeyValue, number uint64, weight big.Int, destructs [][]byte, stateUpdates []storage.KeyValue)
// LatestHash() types.Hash
// NumberToHash(uint64) types.Hash

type layer interface {
  consolidate(hash types.Hash, weight *big.Int, blockData []storage.KeyValue, number uint64, destructs [][]byte, stateUpdates []storage.KeyValue, resumption []byte, merge bool) layer // Consolidates the tree
  getHash() types.Hash
  number() uint64
  getState([]byte, db.Transaction) ([]byte, error)
  getBlockData(types.Hash, []byte, db.Transaction) ([]byte, error)
  parentLayer() layer
  weight() *big.Int
}

type diskLayer struct {
  storage *currentStorage
  h types.Hash
  num uint64
  blockWeight *big.Int
}


func (l *diskLayer) consolidate(hash types.Hash, weight *big.Int, blockData []storage.KeyValue, number uint64, destructs [][]byte, stateUpdates []storage.KeyValue, resumption []byte, merge bool) layer {
  // TODO: Persist the data passed into consolidate() to the disk. Update the
  // hash and num in memory. Persist the details of current layer so that upon
  // resumption the basics of the block can be loaded into memory.
  l.storage.db.Update(func(tr db.Transaction) error {
    numberBytes := make([]byte, 8)
    binary.BigEndian.PutUint64(numberBytes, number)
    tr.Put(HashToNumKey(hash), numberBytes)
    tr.Put(NumToHashKey(number), hash[:])
    tr.Put(ResumptionDataKey, resumption)
    tr.Put(LatestBlockHashKey, hash[:])
    tr.Put(LatestBlockWeightKey, weight.Bytes())
    for _, kv := range(blockData) {
      tr.Put(BlockDataKey(hash, kv.Key), kv.Value)
    }
    for _, kv := range(stateUpdates) {
      tr.Put(StateDataKey(kv.Key), kv.Value)
    }
    for _, destruct := range(destructs) {
      iter := tr.Iterator(StateDataKey(destruct))
      for iter.Next() {
        // NOTE: Some database implementations may not like having keys deleted
        // out of them while they're iterating over them. We may need to
        // compile a list of keys to delete, then delete them after iterating.
        tr.Delete(iter.Key())
      }
    }
    l.h = hash
    l.num = number
    l.blockWeight = weight
    return nil
  })
  return l
}
func (l *diskLayer) getHash() types.Hash {
  return l.h
}
func (l *diskLayer) number() uint64 {
  return l.num
}
func (l *diskLayer) getState(key []byte, tr db.Transaction) ([]byte, error) {
  return tr.Get(StateDataKey(key))
}
func (l *diskLayer) getBlockData(h types.Hash, key []byte, tr db.Transaction) ([]byte, error) {
  return tr.Get(BlockDataKey(h, key))
}
func (l *diskLayer) parentLayer() layer {
  return nil
}

func (l *diskLayer) weight() *big.Int {
  return l.blockWeight
}
