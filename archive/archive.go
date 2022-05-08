package archive

import (
  "bytes"
  "encoding/binary"
  "fmt"
  // "io"
  "math/big"
  "sync"
  "github.com/hashicorp/golang-lru"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/openrelayxyz/cardinal-types/metrics"
  "github.com/openrelayxyz/cardinal-storage"
  "github.com/openrelayxyz/cardinal-storage/db"
  log "github.com/inconshreveable/log15"
  "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/hamba/avro"
)

var (
  partsCache *lru.Cache
	heightGauge = metrics.NewMinorGauge("storage/height")
)

type archiveStorage struct {
  db db.Database
  layers map[types.Hash]layer
  latestHash types.Hash
	lastStoredHash types.Hash
  whitelist map[uint64]types.Hash
  mut sync.RWMutex
  wlock sync.Mutex
	maxDepth int64
}

// New starts a archiveStorage instance from an empty database.
func New(sdb db.Database, maxDepth int64, whitelist map[uint64]types.Hash) (storage.Storage) {
  if whitelist == nil { whitelist = make(map[uint64]types.Hash)}
  if partsCache == nil { partsCache, _ = lru.New(1024) }
  sdb.Update(func(tx db.Transaction) error {
    return tx.Put([]byte("CardinalStorageVersion"), []byte("ArchiveStorage1"))
  })
  s := &archiveStorage{
    db: sdb,
    layers: make(map[types.Hash]layer),
    whitelist: whitelist,
    maxDepth: maxDepth,
    lastStoredHash: types.Hash{},
  }
  s.layers[types.Hash{}] = &archiveLayer{storage: s, num: 0, w: new(big.Int), hash: types.Hash{}}
  return  s
}

// Open loads a archiveStorage instance from a loaded database
func Open(sdb db.Database, maxDepth int64, whitelist map[uint64]types.Hash) (storage.Storage, error) {
  if whitelist == nil { whitelist = make(map[uint64]types.Hash)}
  if partsCache == nil { partsCache, _ = lru.New(1024) }
  s := &archiveStorage{
    db: sdb,
    layers: make(map[types.Hash]layer),
    whitelist: whitelist,
    maxDepth: maxDepth,
  }
  if err := sdb.View(func(tr db.Transaction) error {
    hashBytes, err := tr.Get(LatestBlockHashKey)
    if err != nil {
      log.Error("Error getting latest block hash")
      return err
    }
    s.lastStoredHash = types.BytesToHash(hashBytes)
    numBytes, err := tr.Get(HashToNumKey(s.lastStoredHash))
    if err != nil  {
      log.Error("Error getting block number")
      return err
    }

    if persistenceData, err := tr.Get(MemoryPersistenceKey); err != nil {
      log.Warn("Error reading memory layer. Using disk layer.", "error", err)
      s.latestHash = s.lastStoredHash
    } else {
      s.layers[s.lastStoredHash], err = s.getLayer(s.lastStoredHash)
      if err != nil { return err }
      if h, err := loadMap(s.layers, persistenceData, &s.mut); err != nil {
        log.Warn("Error loading memory layer. Using disk layer.", "error", err)
        s.latestHash = s.lastStoredHash
      } else {
        s.latestHash = h
      }
      delete(s.layers, s.lastStoredHash)
    }
    log.Debug("Initializing current storage", "hash", s.latestHash, "depth", maxDepth, "disknum", binary.BigEndian.Uint64(numBytes), "latestnum", s.layers[s.latestHash].number())
    return nil
  }); err != nil { return nil, err }
  return s, nil
}

func (s *archiveStorage) Close() error {
  s.wlock.Lock() // We're closing, so we're never going to unlock this
  log.Info("Shutting down storage engine")
  data, err := preparePersist(s.layers)
  if err != nil { return err }
  if err := s.db.Update(func(tr db.Transaction) error {
    log.Info("Persisting memory layers")
    return tr.Put(MemoryPersistenceKey, data)
  }); err != nil {
    return err
  }
  return nil
}

// View returns a transaction for interfacing with the layer indicated by the
// specified hash
func (s *archiveStorage) View(hash types.Hash, fn func(storage.Transaction) error) error {
  var once sync.Once
  s.mut.RLock()
  defer once.Do(s.mut.RUnlock) // defer in case there's a DB error for the view. Use once so we don't multiply unlock
  layer, err := s.getLayer(hash)
  switch err {
  case nil:
  case storage.ErrNotFound:
    return storage.ErrLayerNotFound
  default:
    return err
  }
  txlayer := layer.tx()

  return s.db.View(func(tr db.Transaction) error {
    once.Do(s.mut.RUnlock) // We unlock once both the txlayer and db transaction have been created, ensuring that they are synchronized
    ctr := &currentTransaction{
      transaction: tr,
      layer: txlayer,
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
func (s *archiveStorage) AddBlock(hash, parentHash types.Hash, number uint64, weight *big.Int, updates []storage.KeyValue, deletes [][]byte, resumption []byte) error {
  s.wlock.Lock() // Make sure only one thread at a time executes AddBlock()
  defer s.wlock.Unlock()
  if h, ok := s.whitelist[number]; ok && h != hash {
    log.Warn("Ignoring block due to whitelist", "num", number, "wlhash", h, "hash", hash)
    return nil
  }
  parentLayer, err := s.getLayer(parentHash)
  if err != nil {
    log.Info("Not found", "hash", hash, "parent", parentHash)
    return err
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
    resume: resumption,
    children: make(map[types.Hash]*memoryLayer),
    mut: &s.mut,
  }
  for _, kv := range updates {
    newLayer.updatesMap[string(kv.Key)] = kv.Value
  }
  for _, k := range deletes {
    newLayer.deletesMap[string(k)] = struct{}{}
  }
  changes, deletions, err := parentLayer.consolidate(newLayer)
  if err != nil { return err }
  s.mut.Lock()
  // log.Info("latest", "weight", s.latestWeight(), "s", s, "nl", newLayer, "nlw", newLayer.weight())
  if latestWeight := s.latestWeight(); latestWeight.Cmp(newLayer.weight()) < 0 {
    // TODO: Maybe need to use an atomic value for s.latestHash
    log.Debug("New heaviest block", "hash", hash, "number", number, "oldweight", latestWeight, "newweight", newLayer.weight())
    heightGauge.Update(int64(number))
    s.latestHash = hash
  } else {
    log.Debug("Added side block", "hash", hash, "number", number, "headweight", latestWeight, "sideweight", newLayer.weight())
  }
  s.layers[hash] = newLayer
  for h, l := range changes {
    s.layers[h] = l
  }
  for d := range deletions {
    delete(s.layers, d)
  }
  s.mut.Unlock()
  return nil
}

func (s *archiveStorage) getLayer(h types.Hash) (layer, error) {
	if l, ok := s.layers[h]; ok {
		return l, nil
	}
	var number uint64
	if err := s.db.View(func(tr db.Transaction) error {
		return tr.ZeroCopyGet(HashToNumKey(h), func(numberBytes []byte) error {
			number = binary.BigEndian.Uint64(numberBytes)
			return nil
		})
	}); err != nil {
		return nil, err
	}
	return &archiveLayer{storage: s, num: number, hash: h}, nil
}

// Rollback reverts to a specified block number, moving backwards from the
// heaviest known block. This should only be called for reorgs larger than the
// in-memory reorg threshold. Data more recent than the specified block will be
// discarded.
func (s *archiveStorage) Rollback(number uint64) error {
	// return fmt.Errorf("not implemented")
  s.mut.Lock()
  defer s.mut.Unlock() // I don't care if this locks things up for a while. Rollbacks should be very rare.
  currentLayer := s.layers[s.latestHash]
  for currentLayer.number() > number {
    switch l := currentLayer.(type) {
    case rollbackLayer:
      err := l.rollback(number)
      s.latestHash = l.getHash()
      s.layers = map[types.Hash]layer {
        s.latestHash: l,
      }
      return err
    case *memoryLayer:
      for k := range l.descendants() {
        delete(s.layers, k)
      }
      delete(s.layers, l.getHash())
      log.Debug("Rolled back memory layer", "number", currentLayer.number())
      currentLayer = l.parent
      s.latestHash = currentLayer.getHash()
    }
  }
  return nil
}

// LatestHash returns the block with the highest weight added through AddBlock
func (s *archiveStorage) LatestHash() types.Hash {
  return s.latestHash
}

func (s *archiveStorage) latestWeight() *big.Int {
	l, err := s.getLayer(s.latestHash)
	if err != nil { panic("latest layer not available") }
	return l.weight()
}

// NumberToHash returns the hash of the block with the specified number that
// is an ancestor of the block at `LatestHash()`
func (s *archiveStorage) NumberToHash(num uint64) (types.Hash, error) {
  layer := s.layers[s.latestHash]
  if layer.number() < num {
    return types.Hash{}, fmt.Errorf("requested hash of future block %v", num)
  }
  for layer != nil && num < layer.number() {
    layer = layer.parentLayer()
  }
  if layer == nil {
    return types.Hash{}, fmt.Errorf("number %v not availale in history", num)
  }
  if layer.number() != num {
    return types.Hash{}, fmt.Errorf("unknown error occurred finding block number %v (layer %v)", num, layer.number())
  }
  return layer.getHash(), nil
}

// Resumption returns the cardinal-streams resumption token of the latest
// block.
func (s *archiveStorage) LatestBlock() (types.Hash, uint64, *big.Int, []byte) {
  s.mut.RLock()
  latest := s.layers[s.latestHash]
  s.mut.RUnlock()
  return latest.blockInfo()
}

type currentTransaction struct {
  transaction db.Transaction
  layer txlayer
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

type archiveLayer struct {
	storage *archiveStorage
	num uint64
	hash types.Hash
	w *big.Int
	resumption []byte
	children map[types.Hash]*memoryLayer
}

func (l *archiveLayer) consolidate(child *memoryLayer) (map[types.Hash]layer, map[types.Hash]struct{}, error) {
	if l.storage.maxDepth > child.depth {
		return nil, nil, nil
	}
	changes := make(map[types.Hash]layer)
	deletes := make(map[types.Hash]struct{})
	if child.number() != l.num + 1 || l.storage.lastStoredHash != l.hash{
		return nil, nil, fmt.Errorf("out of order consolidation")
	}
	for h, c := range l.children {
		deletes[h] = struct{}{}
		if h != child.getHash() {
			for h := range c.descendants() {
				deletes[h] = struct{}{}
			}
		}
	}

	if err := l.storage.db.View(func(tr db.Transaction) error {
		h, num, weight, resume := child.blockInfo()
		childAl := &archiveLayer{
			storage: l.storage,
			num: num,
			hash: h,
			w: weight,
			resumption: resume,
			children: child.children,
		}
		bw := l.storage.db.BatchWriter()
		bw.Put(LatestBlockHashKey, h.Bytes())
		bw.Put(LatestBlockWeightKey, weight.Bytes())
		bw.Put(NumberToWeightKey(num), weight.Bytes())
		bw.Put(NumberToResumptionKey(num), resume)
		bw.Put(NumToHashKey(num), h.Bytes())
		numberBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(numberBytes, num)
		bw.Put(HashToNumKey(h), numberBytes)
		alteredKeys := make([][]byte, 0, len(child.updates) + len(child.deletes))

		for _, kv := range(child.updates) {
			childAl.put(kv.Key, kv.Value, tr, bw)
			alteredKeys = append(alteredKeys, kv.Key)
		}

		for _, deletKey := range(child.deletes) {
			iter := tr.Iterator(RangeKey(deletKey))
			for iter.Next() {
				k := iter.Key()[1:] // remove the range prefix
				childAl.delete(k, tr, bw)
				alteredKeys = append(alteredKeys, k)
			}
			iter.Close()
		}
		data, err := avro.Marshal(deltaSchema, alteredKeys)
		if err != nil { panic(err) } // TODO: remove after we verify the schema works
		bw.Put(RollbackKey(num), data)
		changes[h] = childAl
		l.storage.lastStoredHash = h
		return bw.Flush()
	}); err != nil {
		return nil, nil, err
	}
	return changes, deletes, nil
}
func (l *archiveLayer) getHash() types.Hash{
	return l.hash
}
func (l *archiveLayer) number() uint64{
	return l.num
}
func (l *archiveLayer) getIndex(k []byte, tr db.Transaction) (uint64, error) {
	// TODO: reason through this given discrepancies between roaring and roaring64's Select implementations
	var index uint64
	if err := tr.ZeroCopyGet(RangeKey(k), func(rangeValue []byte) error {
		bm := new(roaring64.Bitmap)
		if err := bm.UnmarshalBinary(rangeValue); err != nil {
			return err
		}
		// cardinality := bm.GetCardinality()
		index = bm.Rank(l.num)
		return nil
	}); err != nil { return 0, err }
	return index, nil
}
func (l *archiveLayer) get(k []byte, tr db.Transaction) ([]byte, error){
	index, err := l.getIndex(k, tr)
	if err != nil {
		return nil, err
	}
	return tr.Get(DataKey(k, index))
}

func (l *archiveLayer) put(k, v []byte, tr db.Transaction, bw db.BatchWriter) error {
	rk := RangeKey(k)
	if err := tr.ZeroCopyGet(rk, func(rangeValue []byte) error {
		bm := new(roaring64.Bitmap)
		if err := bm.UnmarshalBinary(rangeValue); err != nil {
			return err
		}
		index := bm.GetCardinality() + 1
		bm.Add(l.num)
		bmdata, err := bm.MarshalBinary()
		if err != nil { return err }
		if err := bw.Put(rk, bmdata); err != nil { return err }
		return bw.Put(DataKey(k, index), v)
	}); err == storage.ErrNotFound {
		// First occurrence of this key
		bm := roaring64.BitmapOf(uint64(l.num))
		bmdata, err := bm.MarshalBinary()
		if err != nil { return err }
		if err := bw.Put(rk, bmdata); err != nil { return err }
		return bw.Put(DataKey(k, 1), v)
	}
	return nil
}

func (l *archiveLayer) delete(k []byte, tr db.Transaction, bw db.BatchWriter) error {
	// We can "delete" a record by adding the new block number to the end of the
	// bitmap without creating a corresponding data entry. During reads, the
	// index will be found, but trying to read the data key for that index will
	// yield ErrNoError, which is what we want after a delete.
	rk := RangeKey(k)
	if err := tr.ZeroCopyGet(rk, func(rangeValue []byte) error {
		bm := new(roaring64.Bitmap)
		if err := bm.UnmarshalBinary(rangeValue); err != nil {
			return err
		}
		bm.Add(l.num)
		bmdata, err := bm.MarshalBinary()
		if err != nil { return err }
		return bw.Put(rk, bmdata)
	}); err != nil {
		// If the range key never existed, we don't need to record anything
		return nil
	}
	return nil
}



func (l *archiveLayer) zeroCopyGet(k []byte, tr db.Transaction, fn func(v []byte) error) (error){
	index, err := l.getIndex(k, tr)
	if err != nil { return err }
	return tr.ZeroCopyGet(DataKey(k, index), fn)
}
func (l *archiveLayer) parentLayer() layer{
	return nil
}
func (l *archiveLayer) weight() *big.Int {
	// log.Debug("weight()", "w", l.w, "n", l.w == nil)
	if l.w == nil {
		if err := l.storage.db.View(func(tr db.Transaction) error {
			return tr.ZeroCopyGet(NumberToWeightKey(l.num), func(weightBytes []byte) error {
				l.w = new(big.Int).SetBytes(weightBytes)
				return nil
			})
		}); err != nil { return nil }
	}
	return l.w
}
func (l *archiveLayer) numberToHash(num uint64, tr db.Transaction) types.Hash{
	if num == l.num { return l.hash }
	var hash types.Hash
	if err := l.storage.db.View(func(tr db.Transaction) error {
		return tr.ZeroCopyGet(NumToHashKey(num), func(hashBytes []byte) error {
			copy(hash[:], hashBytes[:])
			return nil
		})
	}); err != nil {
		return hash
	}
	return hash
}
func (l *archiveLayer) hashToNumber(hash types.Hash, tr db.Transaction) uint64{
	if hash == l.hash { return l.num }
	var number uint64
	if err := l.storage.db.View(func(tr db.Transaction) error {
		return tr.ZeroCopyGet(HashToNumKey(hash), func(numberBytes []byte) error {
			number = binary.BigEndian.Uint64(numberBytes)
			return nil
		})
	}); err != nil {
		return 0
	}
	return number
}
func (l *archiveLayer) tx() txlayer{
	return l
}
func (l *archiveLayer) blockInfo() (types.Hash, uint64, *big.Int, []byte){
	if len(l.resumption) == 0 {
		l.storage.db.View(func(tr db.Transaction) error {
			l.resumption, _ = tr.Get(NumberToResumptionKey(l.num))
			return nil
		})
	}
	return l.hash, l.num, l.weight(), l.resumption
}
func (l *archiveLayer) persisted() bool{
	return true
}
func (l *archiveLayer) rollback(target uint64) error {
	return l.storage.db.View(func(tr db.Transaction) error {
		bw := l.storage.db.BatchWriter()
		for i := l.num ; i > target; i-- {
			data, err := tr.Get(RollbackKey(i))
			if err != nil { return err }
			var alteredKeys [][]byte
			err = avro.Unmarshal(deltaSchema, data, &alteredKeys)
			if err != nil { return err }
			for _, k := range alteredKeys {
				var index uint64
				if err := tr.ZeroCopyGet(RangeKey(k), func(rangeValue []byte) error {
					bm := new(roaring64.Bitmap)
					if err := bm.UnmarshalBinary(rangeValue); err != nil {
						return err
					}
					index := bm.GetCardinality()
					bm.Remove(index)
					bmdata, err := bm.MarshalBinary()
					if err != nil { return err }
					return bw.Put(RangeKey(k), bmdata)
				}); err != nil { return err }
				bw.Delete(DataKey(k, index))
				bw.Delete(RollbackKey(i))
			}
		}
		return nil
	})
}

type memoryLayer struct {
  parent layer
  num uint64
  hash types.Hash
  blockWeight *big.Int
  updatesMap map[string][]byte
  updates []storage.KeyValue
  deletesMap map[string]struct{}
  deletes [][]byte
  resume []byte
  depth int64
  children map[types.Hash]*memoryLayer
  mut *sync.RWMutex
}

func (l *memoryLayer) consolidate(child *memoryLayer) (map[types.Hash]layer, map[types.Hash]struct{}, error){
  if child.depth + 1 > l.depth {
    l.depth = child.depth + 1
  }
  l.children[child.hash] = child
  changes, deletes, err := l.parent.consolidate(l)
  if newParent, ok := changes[l.parent.getHash()]; ok {
    l.mut.Lock()
    // We hold the lock for a remarkably small period of time, but ensure
    // atomic views
    l.parent = newParent
    l.mut.Unlock()
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
func (l *memoryLayer) persisted() bool {
  return false
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
func (l *memoryLayer) blockInfo() (types.Hash, uint64, *big.Int, []byte) {
  return l.hash, l.num, l.blockWeight, l.resume
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
  if len(l.deletesMap) == 0 {
    return false
  }
  parts := []string{}
  if v, ok := partsCache.Get(string(key)); ok {
    parts = v.([]string)
  } else {
    components := bytes.Split(key, []byte("/"))
    for i := 1; i < len(components); i++ {
      parts = append(parts, string(bytes.Join(components[:i], []byte("/"))))
      partsCache.Add(string(key), parts)
    }
  }
  for _, part := range parts {
    if _, ok := l.deletesMap[part]; ok {
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

func (l *memoryLayer) tx() txlayer {
  return &memtxlayer{
    parent: l.parent.tx(),
    num: l.num,
    hash: l.hash,
    updatesMap: l.updatesMap,
    deletesMap: l.deletesMap,
  }
}

type memtxlayer struct{
  parent txlayer
  num uint64
  hash types.Hash
  updatesMap map[string][]byte
  deletesMap map[string]struct{}
}

func (l *memtxlayer) get(key []byte, tr db.Transaction) ([]byte, error) {
  if l.isDeleted(key) {
    return []byte{}, storage.ErrNotFound
  }
  if val, ok := l.updatesMap[string(key)]; ok {
    return val, nil
  }
  return l.parent.get(key, tr)
}

func (l *memtxlayer) zeroCopyGet(key []byte, tr db.Transaction, fn func([]byte) error) error {
  if l.isDeleted(key) {
    return storage.ErrNotFound
  }
  if val, ok := l.updatesMap[string(key)]; ok {
    return fn(val)
  }
  return l.parent.zeroCopyGet(key, tr, fn)
}

func (l *memtxlayer) isDeleted(key []byte) bool {
  if len(l.deletesMap) == 0 {
    return false
  }
  parts := []string{}
  if v, ok := partsCache.Get(string(key)); ok {
    parts = v.([]string)
  } else {
    components := bytes.Split(key, []byte("/"))
    for i := 1; i < len(components); i++ {
      parts = append(parts, string(bytes.Join(components[:i], []byte("/"))))
      partsCache.Add(string(key), parts)
    }
  }
  for _, part := range parts {
    if _, ok := l.deletesMap[part]; ok {
      return true
    }
  }
  return false
}


func (l *memtxlayer) numberToHash(x uint64, tr db.Transaction) types.Hash {
  if l.num == x {
    return l.hash
  }
  return l.parent.numberToHash(x, tr)
}

func (l *memtxlayer) hashToNumber(x types.Hash, tr db.Transaction) uint64 {
  if l.hash == x {
    return l.num
  }
  return l.parent.hashToNumber(x, tr)
}


type txlayer interface {
  get([]byte, db.Transaction) ([]byte, error)
  zeroCopyGet([]byte, db.Transaction, func([]byte) error) (error)
  numberToHash(uint64, db.Transaction) types.Hash
  hashToNumber(types.Hash, db.Transaction) uint64
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
  tx() txlayer
  blockInfo() (types.Hash, uint64, *big.Int, []byte)
  persisted() bool
}

type rollbackLayer interface {
  layer
  rollback(uint64) error
}
