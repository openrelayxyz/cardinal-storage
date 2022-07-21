package current

import (
	"bytes"
	"encoding/binary"
	"fmt"
	// "io"
	"github.com/hashicorp/golang-lru"
	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-storage"
	"github.com/openrelayxyz/cardinal-storage/db"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/metrics"
	"math/big"
	"sync"
)

var (
	partsCache  *lru.Cache
	heightGauge = metrics.NewMinorGauge("storage/height")
)

type currentStorage struct {
	db         db.Database
	layers     map[types.Hash]layer
	latestHash types.Hash
	whitelist  map[uint64]types.Hash
	mut        sync.RWMutex
	wlock      sync.Mutex
}

// New starts a currentStorage instance from an empty database.
func New(sdb db.Database, maxDepth int64, whitelist map[uint64]types.Hash) storage.Storage {
	if whitelist == nil {
		whitelist = make(map[uint64]types.Hash)
	}
	if partsCache == nil {
		partsCache, _ = lru.New(1024)
	}
	sdb.Update(func(tx db.Transaction) error {
		return tx.Put([]byte("CardinalStorageVersion"), []byte("CurrentStorage1"))
	})
	s := &currentStorage{
		db:        sdb,
		layers:    make(map[types.Hash]layer),
		whitelist: whitelist,
	}
	s.layers[types.Hash{}] = &diskLayer{
		storage:     s,
		h:           types.Hash{},
		num:         0,
		blockWeight: new(big.Int),
		children:    make(map[types.Hash]*memoryLayer),
		depth:       0,
		maxDepth:    maxDepth,
	}
	return s
}

// Open loads a currentStorage instance from a loaded database
func Open(sdb db.Database, maxDepth int64, whitelist map[uint64]types.Hash) (storage.Storage, error) {
	if whitelist == nil {
		whitelist = make(map[uint64]types.Hash)
	}
	if partsCache == nil {
		partsCache, _ = lru.New(1024)
	}
	s := &currentStorage{
		db:        sdb,
		layers:    make(map[types.Hash]layer),
		whitelist: whitelist,
	}
	if err := sdb.View(func(tr db.Transaction) error {
		hashBytes, err := tr.Get(LatestBlockHashKey)
		if err != nil {
			log.Error("Error getting latest block hash")
			return err
		}
		blockHash := types.BytesToHash(hashBytes)
		numBytes, err := tr.Get(HashToNumKey(blockHash))
		if err != nil {
			log.Error("Error getting block number")
			return err
		}
		weightBytes, err := tr.Get(LatestBlockWeightKey)
		if err != nil {
			log.Error("Error getting block weight")
			return err
		}
		resumptionBytes, err := tr.Get(ResumptionDataKey)
		if err != nil && err != storage.ErrNotFound {
			log.Error("Error getting resumption key")
			return err
		}

		s.layers[blockHash] = &diskLayer{
			storage:     s,
			h:           blockHash,
			num:         binary.BigEndian.Uint64(numBytes),
			blockWeight: new(big.Int).SetBytes(weightBytes),
			children:    make(map[types.Hash]*memoryLayer),
			depth:       0,
			maxDepth:    maxDepth,
			resume:      resumptionBytes,
		}
		if persistenceData, err := tr.Get(MemoryPersistenceKey); err != nil {
			log.Warn("Error reading memory layer. Using disk layer.", "error", err)
			s.latestHash = blockHash
		} else {
			if h, err := loadMap(s.layers, persistenceData, &s.mut); err != nil {
				log.Warn("Error loading memory layer. Using disk layer.", "error", err)
				s.latestHash = blockHash
			} else {
				s.latestHash = h
			}
		}
		log.Debug("Initializing current storage", "hash", s.latestHash, "depth", maxDepth, "disknum", binary.BigEndian.Uint64(numBytes), "latestnum", s.layers[s.latestHash].number())
		return nil
	}); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *currentStorage) Close() error {
	s.wlock.Lock() // We're closing, so we're never going to unlock this
	log.Info("Shutting down storage engine")
	data, err := preparePersist(s.layers)
	if err != nil {
		return err
	}
	if err := s.db.Update(func(tr db.Transaction) error {
		log.Info("Persisting memory layers")
		return tr.Put(MemoryPersistenceKey, data)
	}); err != nil {
		return err
	}
	s.db.Close()
	return nil
}

// View returns a transaction for interfacing with the layer indicated by the
// specified hash
func (s *currentStorage) View(hash types.Hash, fn func(storage.Transaction) error) error {
	var once sync.Once
	s.mut.RLock()
	defer once.Do(s.mut.RUnlock) // defer in case there's a DB error for the view. Use once so we don't multiply unlock
	layer, ok := s.layers[hash]
	if !ok {
		return storage.ErrLayerNotFound
	}
	txlayer := layer.tx()

	return s.db.View(func(tr db.Transaction) error {
		once.Do(s.mut.RUnlock) // We unlock once both the txlayer and db transaction have been created, ensuring that they are synchronized
		ctr := &currentTransaction{
			transaction: tr,
			layer:       txlayer,
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
	s.wlock.Lock() // Make sure only one thread at a time executes AddBlock()
	defer s.wlock.Unlock()
	if h, ok := s.whitelist[number]; ok && h != hash {
		log.Warn("Ignoring block due to whitelist", "num", number, "wlhash", h, "hash", hash)
		return nil
	}
	parentLayer, ok := s.layers[parentHash]
	if !ok {
		return storage.ErrLayerNotFound
	}
	newLayer := &memoryLayer{
		hash:        hash,
		parent:      parentLayer,
		num:         number,
		blockWeight: weight,
		updatesMap:  make(map[string][]byte),
		updates:     updates,
		deletesMap:  make(map[string]struct{}),
		deletes:     deletes,
		resume:      resumption,
		children:    make(map[types.Hash]*memoryLayer),
		mut:         &s.mut,
	}
	for _, kv := range updates {
		newLayer.updatesMap[string(kv.Key)] = kv.Value
	}
	for _, k := range deletes {
		newLayer.deletesMap[string(k)] = struct{}{}
	}
	changes, deletions, err := parentLayer.consolidate(newLayer)
	if err != nil {
		return err
	}
	s.mut.Lock()
	if parentHash == s.latestHash || s.layers[s.latestHash].weight().Cmp(newLayer.weight()) < 0 {
		// TODO: Maybe need to use an atomic value for s.latestHash
		log.Debug("New heaviest block", "hash", hash, "number", number, "oldweight", s.layers[s.latestHash].weight(), "newweight", newLayer.weight())
		heightGauge.Update(int64(number))
		s.latestHash = hash
	} else {
		log.Debug("Added side block", "hash", hash, "number", number, "headweight", s.layers[s.latestHash].weight(), "sideweight", newLayer.weight())
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

// Rollback reverts to a specified block number, moving backwards from the
// heaviest known block. This should only be called for reorgs larger than the
// in-memory reorg threshold. Data more recent than the specified block will be
// discarded.
func (s *currentStorage) Rollback(number uint64) error {
	s.mut.Lock()
	defer s.mut.Unlock() // I don't care if this locks things up for a while. Rollbacks should be very rare.
	currentLayer := s.layers[s.latestHash]
	for currentLayer.number() > number {
		switch l := currentLayer.(type) {
		case rollbackLayer:
			err := l.rollback(number)
			s.latestHash = l.getHash()
			s.layers = map[types.Hash]layer{
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
func (s *currentStorage) LatestHash() types.Hash {
	return s.latestHash
}

// NumberToHash returns the hash of the block with the specified number that
// is an ancestor of the block at `LatestHash()`
func (s *currentStorage) NumberToHash(num uint64) (types.Hash, error) {
	s.mut.RLock()
	layer := s.layers[s.latestHash]
	s.mut.RUnlock()
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
func (s *currentStorage) LatestBlock() (types.Hash, uint64, *big.Int, []byte) {
	s.mut.RLock()
	latest := s.layers[s.latestHash]
	s.mut.RUnlock()
	return latest.blockInfo()
}

type currentTransaction struct {
	transaction db.Transaction
	layer       txlayer
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

type memoryLayer struct {
	parent      layer
	num         uint64
	hash        types.Hash
	blockWeight *big.Int
	updatesMap  map[string][]byte
	updates     []storage.KeyValue
	deletesMap  map[string]struct{}
	deletes     [][]byte
	resume      []byte
	depth       int64
	children    map[types.Hash]*memoryLayer
	mut         *sync.RWMutex
}

func (l *memoryLayer) consolidate(child *memoryLayer) (map[types.Hash]layer, map[types.Hash]struct{}, error) {
	if child.depth+1 > l.depth {
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
	if val, ok := l.updatesMap[string(key)]; ok {
		return val, nil
	}
	if l.isDeleted(key) {
		return []byte{}, storage.ErrNotFound
	}
	return l.parent.get(key, tr)
}

func (l *memoryLayer) zeroCopyGet(key []byte, tr db.Transaction, fn func([]byte) error) error {
	if val, ok := l.updatesMap[string(key)]; ok {
		return fn(val)
	}
	if l.isDeleted(key) {
		return storage.ErrNotFound
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
		parent:     l.parent.tx(),
		num:        l.num,
		hash:       l.hash,
		updatesMap: l.updatesMap,
		deletesMap: l.deletesMap,
	}
}

type memtxlayer struct {
	parent     txlayer
	num        uint64
	hash       types.Hash
	updatesMap map[string][]byte
	deletesMap map[string]struct{}
}

func (l *memtxlayer) get(key []byte, tr db.Transaction) ([]byte, error) {
	if val, ok := l.updatesMap[string(key)]; ok {
		return val, nil
	}
	if l.isDeleted(key) {
		return []byte{}, storage.ErrNotFound
	}
	return l.parent.get(key, tr)
}

func (l *memtxlayer) zeroCopyGet(key []byte, tr db.Transaction, fn func([]byte) error) error {
	if val, ok := l.updatesMap[string(key)]; ok {
		return fn(val)
	}
	if l.isDeleted(key) {
		return storage.ErrNotFound
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
	zeroCopyGet([]byte, db.Transaction, func([]byte) error) error
	numberToHash(uint64, db.Transaction) types.Hash
	hashToNumber(types.Hash, db.Transaction) uint64
}

type layer interface {
	consolidate(*memoryLayer) (map[types.Hash]layer, map[types.Hash]struct{}, error) // Consolidates the tree
	getHash() types.Hash
	number() uint64
	get([]byte, db.Transaction) ([]byte, error)
	zeroCopyGet([]byte, db.Transaction, func([]byte) error) error
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

type diskLayer struct {
	storage     *currentStorage
	h           types.Hash
	num         uint64
	blockWeight *big.Int
	children    map[types.Hash]*memoryLayer
	depth       int64
	maxDepth    int64
	resume      []byte
}

func (l *diskLayer) consolidate(child *memoryLayer) (map[types.Hash]layer, map[types.Hash]struct{}, error) {
	if child.depth+1 > l.depth {
		l.depth = child.depth + 1
	}
	changes := make(map[types.Hash]layer)
	deletes := make(map[types.Hash]struct{})
	if l.depth > l.maxDepth {
		for childHash, childLayer := range l.children {
			if childHash == child.hash {
				continue
			}
			for h := range childLayer.descendants() {
				deletes[h] = struct{}{}
			}
			deletes[childHash] = struct{}{}
		}
		deletes[l.getHash()] = struct{}{}
		changes[child.hash] = l
		if err := l.storage.db.View(func(tr db.Transaction) error {
			delta := newDelta(tr, l.storage.db.BatchWriter())
			numberBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(numberBytes, child.number())
			delta.put(HashToNumKey(child.hash), numberBytes)
			delta.put(NumToHashKey(child.number()), child.hash[:])
			delta.put(ResumptionDataKey, child.resume)

			delta.put(LatestBlockHashKey, child.hash[:])
			delta.put(LatestBlockWeightKey, child.weight().Bytes())
			for _, deletKey := range child.deletes {
				iter := tr.Iterator(DataKey(deletKey))
				for iter.Next() {
					delta.delete(iter.Key())
				}
				iter.Close()
			}
			for _, kv := range child.updates {
				delta.put(DataKey(kv.Key), kv.Value)
			}
			l.h = child.hash
			l.num = child.number()
			l.blockWeight = child.weight()
			l.depth = child.depth
			l.children = child.children
			l.resume = child.resume
			return delta.finalize(child.number())
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
func (l *diskLayer) blockInfo() (types.Hash, uint64, *big.Int, []byte) {
	return l.h, l.num, l.blockWeight, l.resume
}

func (l *diskLayer) zeroCopyGet(key []byte, tr db.Transaction, fn func([]byte) error) error {
	return tr.ZeroCopyGet(DataKey(key), fn)
}
func (l *diskLayer) parentLayer() layer {
	return nil
}

func (l *diskLayer) persisted() bool {
	return true
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

func (l *diskLayer) tx() txlayer {
	// All of the relevant txlayer methods run off of the transaction for the
	// disk layer, so we can just pass the unmodified disk layer.
	return l
}

func (l *diskLayer) rollback(number uint64) error {
	l.children = make(map[types.Hash]*memoryLayer)
	l.depth = 0
	for l.num > number {
		if err := l.storage.db.View(func(tr db.Transaction) error {
			d, err := loadDelta(l.num, tr, l.storage.db.BatchWriter())
			if err != nil {
				return fmt.Errorf("error loading delta: %v", err)
			}
			if err := d.apply(); err != nil {
				return fmt.Errorf("error applying delta: %v", err)
			}
			l.num--
			if err := tr.ZeroCopyGet(NumToHashKey(l.num), func(data []byte) error {
				copy(l.h[:], data[:])
				return nil
			}); err != nil {
				return fmt.Errorf("error getting key %x: %v", string(NumToHashKey(l.num)), err.Error())
			}
			if err := tr.ZeroCopyGet(LatestBlockWeightKey, func(data []byte) error {
				l.blockWeight.SetBytes(data)
				return nil
			}); err != nil {
				return fmt.Errorf("error getting key %x: %v", string(LatestBlockWeightKey), err.Error())
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}
