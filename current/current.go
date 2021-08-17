package current

import (
  "bytes"
  "encoding/binary"
  "fmt"
  "math/big"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/openrelayxyz/cardinal-storage"
  "github.com/openrelayxyz/cardinal-storage/db"
  log "github.com/inconshreveable/log15"
)

// TODO: We should have a way to specify `number = hash` for some set of
// blocks, similar to Geth's --whitelist flag. This would make sure that in the
// event of a reorg larger than maxDepth we could manually indicate which side
// of the split to be on and ignore blocks on the other side of the split.
type currentStorage struct {
  db db.Database
  layers map[types.Hash]layer
  latestHash types.Hash
  whitelist map[uint64]types.Hash
}

// New starts a currentStorage instance from an empty database.
func New(sdb db.Database, maxDepth int64, whitelist map[uint64]types.Hash) (storage.Storage) {
  if whitelist == nil { whitelist = make(map[uint64]types.Hash)}
  s := &currentStorage{
    db: sdb,
    layers: make(map[types.Hash]layer),
    whitelist: whitelist,
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
    return storage.ErrLayerNotFound
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
func (s *currentStorage) AddBlock(hash, parentHash types.Hash, number uint64, weight *big.Int, updates []storage.KeyValue, deletes [][]byte, resumption []byte) error {
  if h, ok := s.whitelist[number]; ok && h != hash {
    log.Warn("Ignoring block due to whitelist", "num", number, "wlhash", h, "hash", hash)
    return nil
  }
  parentLayer, ok := s.layers[parentHash]
  if !ok {
    return storage.ErrLayerNotFound
  }
  newLayer := &memoryLayer{
    hash: hash,
    parent: parentLayer,
    num: number,
    blockWeight: weight,
    updatesMap: make(map[string][]byte),
    updates: updates,
    deletesMap: make(map[string]struct{}),
    deletes: deletes,
    resumption: resumption,
    children: make(map[types.Hash]*memoryLayer),
  }
  s.layers[hash] = newLayer
  for _, kv := range updates {
    newLayer.updatesMap[string(kv.Key)] = kv.Value
  }
  for _, k := range deletes {
    newLayer.deletesMap[string(k)] = struct{}{}
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
    return types.Hash{}, fmt.Errorf("requested hash of future block %v", num)
  }
  for layer != nil && num > layer.number() {
    layer = layer.parentLayer()
  }
  if layer == nil {
    return types.Hash{}, fmt.Errorf("number %v not availale in history", num)
  }
  if layer.number() != num {
    return types.Hash{}, fmt.Errorf("unknown error occurred finding block number %v", num)
  }
  return layer.getHash(), nil
}

type currentTransaction struct {
  transaction db.Transaction
  layer layer
}

func (t *currentTransaction) Get(key []byte) ([]byte, error) {
  return t.layer.get(key, t.transaction)
}

func (t *currentTransaction) ZeroCopyGet(key []byte, fn func([]byte) error) error {
  return t.layer.zeroCopyGet(key, t.transaction, fn)
}

func (t *currentTransaction) NumberToHash(num uint64) types.Hash {
  return t.layer.numberToHash(num, t.transaction)
}

func (t *currentTransaction) HashToNumber(hash types.Hash) uint64 {
  return t.layer.hashToNumber(hash, t.transaction)
}

// type Storage interface {
//   View(types.Hash, func(Transaction) error)
//   AddBlock(hash, parentHash types.Hash, blockData []storage.KeyValue, number uint64, weight big.Int, deletes [][]byte, stateUpdates []storage.KeyValue)
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
  updatesMap map[string][]byte
  updates []storage.KeyValue
  deletesMap map[string]struct{}
  deletes [][]byte
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

func (l *memoryLayer) get(key []byte, tr db.Transaction) ([]byte, error) {
  if l.isDeleted(key) {
    return []byte{}, storage.ErrNotFound
  }
  if val, ok := l.updatesMap[string(key)]; ok {
    return val, nil
  }
  return l.parent.get(key, tr)
}

func (l *memoryLayer) zeroCopyGet(key []byte, tr db.Transaction, fn func([]byte) error) error {
  if l.isDeleted(key) {
    return storage.ErrNotFound
  }
  if val, ok := l.updatesMap[string(key)]; ok {
    return fn(val)
  }
  return l.parent.zeroCopyGet(key, tr, fn)
}

func (l *memoryLayer) isDeleted(key []byte) bool {
  components := bytes.Split(key, []byte("/"))
  for i := 1; i <= len(components); i++ {
    if _, ok := l.deletesMap[string(bytes.Join(components[:i], []byte("/")))]; ok {
      return true
    }
  }
  return false
}


func (l *memoryLayer) numberToHash(x uint64, tr db.Transaction) types.Hash {
  if l.num == x {
    return l.hash
  }
  return l.parent.numberToHash(x, tr)
}

func (l *memoryLayer) hashToNumber(x types.Hash, tr db.Transaction) uint64 {
  if l.hash == x {
    return l.num
  }
  return l.parent.hashToNumber(x, tr)
}


type layer interface {
  consolidate(*memoryLayer) (map[types.Hash]layer, map[types.Hash]struct{}, error) // Consolidates the tree
  getHash() types.Hash
  number() uint64
  get([]byte, db.Transaction) ([]byte, error)
  zeroCopyGet([]byte, db.Transaction, func([]byte) error) (error)
  parentLayer() layer
  weight() *big.Int
  numberToHash(uint64, db.Transaction) types.Hash
  hashToNumber(types.Hash, db.Transaction) uint64
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
      for _, kv := range(child.updates) {
        tr.Put(DataKey(kv.Key), kv.Value)
      }
      for _, deletKey := range(child.deletes) {
        iter := tr.Iterator(DataKey(deletKey))
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
func (l *diskLayer) get(key []byte, tr db.Transaction) ([]byte, error) {
  return tr.Get(DataKey(key))
}

func(l *diskLayer) zeroCopyGet(key []byte, tr db.Transaction, fn func([]byte) error) error {
  return tr.ZeroCopyGet(key, fn)
}
func (l *diskLayer) parentLayer() layer {
  return nil
}

func (l *diskLayer) weight() *big.Int {
  return l.blockWeight
}
func (l *diskLayer) numberToHash(x uint64, tr db.Transaction) types.Hash {
  var hash types.Hash
  tr.ZeroCopyGet(NumToHashKey(x), func(data []byte) error {
    copy(hash[:], data)
    return nil
  })
  return hash
}

func (l *diskLayer) hashToNumber(x types.Hash, tr db.Transaction) uint64 {
  var result uint64
  tr.ZeroCopyGet(HashToNumKey(x), func(numberBytes []byte) error {
    result = binary.BigEndian.Uint64(numberBytes)
    return nil
  })
  return result
}
