package current

import (
  "bytes"
  "encoding/binary"
  "fmt"
  "math/big"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/openrelayxyz/cardinal-storage"
  "github.com/openrelayxyz/cardinal-storage/db"
  // "log"
)

// TODO: We should have a way to specify `number = hash` for some set of
// blocks, similar to Geth's --whitelist flag. This would make sure that in the
// event of a reorg larger than maxDepth we could manually indicate which side
// of the split to be on and ignore blocks on the other side of the split.
type currentStorage struct {
  db db.Database
  layers map[types.Hash]layer
  latestHash types.Hash
}

// New starts a currentStorage instance from an empty database.
func New(sdb db.Database, maxDepth int64) (storage.Storage) {
  s := &currentStorage{
    db: sdb,
    layers: make(map[types.Hash]layer),
  }
  s.layers[types.Hash{}] = &diskLayer{
    storage: s,
    h: types.Hash{},
    num: 0,
    blockWeight: new(big.Int),
    children: make(map[types.Hash]*memoryLayer),
    depth: 0,
    maxDepth: maxDepth,
  }
  return  s
}

// Open loads a currentStorage instance from a loaded database
func Open(sdb db.Database, maxDepth int64) (storage.Storage, error) {
  s := &currentStorage{
    db: sdb,
    layers: make(map[types.Hash]layer),
  }
  if err := sdb.View(func(tr db.Transaction) error {
    hashBytes, err := tr.Get(LatestBlockHashKey)
    if err != nil { return err }
    blockHash := types.BytesToHash(hashBytes)
    numBytes, err := tr.Get(HashToNumKey(blockHash))
    if err != nil  { return err }
    weightBytes, err := tr.Get(LatestBlockWeightKey)
    if err != nil  { return err }
    s.layers[blockHash] = &diskLayer{
      storage: s,
      h: blockHash,
      num: binary.BigEndian.Uint64(numBytes),
      blockWeight: new(big.Int).SetBytes(weightBytes),
      children: make(map[types.Hash]*memoryLayer),
      depth: 0,
      maxDepth: maxDepth,
   }
   return nil
  }); err != nil { return nil, err }
  return s, nil
}

// View returns a transaction for interfacing with the layer indicated by the
// specified hash
func (s *currentStorage) View(hash types.Hash, fn func(storage.Transaction) error) error {
  layer, ok := s.layers[hash]
  if !ok {
    return ErrLayerNotFound
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
    return ErrLayerNotFound
  }
  newLayer := &memoryLayer{
    hash: hash,
    parent: parentLayer,
    num: number,
    blockWeight: weight,
    blockDataMap: make(map[string][]byte),
    stateUpdatesMap: make(map[string][]byte),
    stateUpdates: stateUpdates,
    blockData: blockData,
    destructsMap: make(map[string]bool),
    destructs: destructs,
    resumption: resumption,
    children: make(map[types.Hash]*memoryLayer),
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
  changes, deletions, err := parentLayer.consolidate(newLayer)
  if err != nil { return err }
  for h, l := range changes {
    s.layers[h] = l
  }
  for d := range deletions {
    delete(s.layers, d)
  }
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
    return types.Hash{}, fmt.Errorf("Requested hash of future block %v", num)
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
  depth int64
  children map[types.Hash]*memoryLayer
}

func (l *memoryLayer) consolidate(child *memoryLayer) (map[types.Hash]layer, map[types.Hash]struct{}, error){
  if child.depth + 1 > l.depth {
    l.depth = child.depth + 1
  }
  l.children[child.hash] = child
  changes, deletes, err := l.parent.consolidate(l)
  if newParent, ok := changes[l.parent.getHash()]; ok {
    l.parent = newParent
  }
  return changes, deletes, err
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
func (l *memoryLayer) descendants() map[types.Hash]struct{} {
  descendants := make(map[types.Hash]struct{})
  for hash, child := range l.children {
    descendants[hash] = struct{}{}
    for hash := range child.descendants() {
      descendants[hash] = struct{}{}
    }
  }
  return descendants
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
  consolidate(*memoryLayer) (map[types.Hash]layer, map[types.Hash]struct{}, error) // Consolidates the tree
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
  children map[types.Hash]*memoryLayer
  depth int64
  maxDepth int64
}


func (l *diskLayer) consolidate(child *memoryLayer) (map[types.Hash]layer, map[types.Hash]struct{}, error) {
  if child.depth + 1 > l.depth {
    l.depth = child.depth + 1
  }
  changes := make(map[types.Hash]layer)
  deletes := make(map[types.Hash]struct{})
  if l.depth > l.maxDepth {
    for childHash, childLayer := range l.children {
      if childHash == child.hash { continue }
      for h := range childLayer.descendants() {
        deletes[h] = struct{}{}
      }
      deletes[childHash] = struct{}{}
    }
    deletes[l.getHash()] = struct{}{}
    changes[child.hash] = l
    if err := l.storage.db.Update(func(tr db.Transaction) error {
      numberBytes := make([]byte, 8)
      binary.BigEndian.PutUint64(numberBytes, child.number())
      tr.Put(HashToNumKey(child.hash), numberBytes)
      tr.Put(NumToHashKey(child.number()), child.hash[:])
      tr.Put(ResumptionDataKey, child.resumption)
      tr.Put(LatestBlockHashKey, child.hash[:])
      tr.Put(LatestBlockWeightKey, child.weight().Bytes())
      for _, kv := range(child.blockData) {
        tr.Put(BlockDataKey(child.hash, kv.Key), kv.Value)
      }
      for _, kv := range(child.stateUpdates) {
        tr.Put(StateDataKey(kv.Key), kv.Value)
      }
      for _, destruct := range(child.destructs) {
        iter := tr.Iterator(StateDataKey(destruct))
        for iter.Next() {
          // NOTE: Some database implementations may not like having keys deleted
          // out of them while they're iterating over them. We may need to
          // compile a list of keys to delete, then delete them after iterating.
          tr.Delete(iter.Key())
        }
      }
      l.h = child.hash
      l.num = child.number()
      l.blockWeight = child.weight()
      l.depth = child.depth
      l.children = child.children
      return nil
    }); err != nil {
      return nil, nil, err
    }
  }
  return changes, deletes, nil
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

// TODO: Consider how to do an initial load of a large quantity of data into
// the disk layer.
