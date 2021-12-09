package current

import (
	"fmt"
	"math/big"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-storage"
	"github.com/hamba/avro"
	"sync"
)

var (
	persistLayerSchema = avro.MustParse(`{
		"type": "array",
		"name": "memLayers",
		"avro.codec": "snappy",
		"namespace": "cloud.rivet.cardinal.storage",
		"items": {
			"name": "layer",
			"type": "record",
			"fields": [
				{"name": "parent", "type": {"name": "par", "type": "fixed", "size": 32}},
				{"name": "number", "type": "long"},
				{"name": "hash", "type": {"name": "h", "type": "fixed", "size": 32}},
				{"name": "weight", "type": "bytes"},
				{"name": "updates", "type": {
					"type": "array",
					"items": {
						"name": "update",
						"type": "record",
						"fields": [
						{"name": "key", "type": "bytes"},
						{"name": "value", "type": "bytes"}
						]
					}
				}},
				{"name": "deletes", "type": {
					"type": "array",
					"items": "bytes"
				}},
				{"name": "resume", "type": "bytes"},
				{"name": "children", "type": {
					"type": "array",
					"items": {
						"name": "child",
						"type": "fixed",
						"size": 32
					}
				}}
			]
		}
	}`)
)


type memLayerPersist struct{
	Parent   types.Hash `avro:"parent"`
	Number   int64      `avro:"number"`
	Hash     types.Hash `avro:"hash"`
	Weight   []byte     `avro:"weight"`
	Updates  []storage.KeyValue `avro:"updates"`
	Deletes  [][]byte   `avro:"deletes"`
	Resume   []byte     `avro:"resume"`
	Children []types.Hash `avro:"children"`
}

func preparePersist(layers map[types.Hash]layer) ([]byte, error) {
	persistLayers := make([]memLayerPersist, 0, len(layers))
	for _, v := range layers {
		switch l := v.(type) {
		case *memoryLayer:
			children := make([]types.Hash, len(l.children))
			for k := range l.children {
				children = append(children, k)
			}
			persistLayers = append(persistLayers, memLayerPersist{
				Parent: l.parentLayer().getHash(),
				Number: int64(l.num),
				Hash: l.hash,
				Weight: l.blockWeight.Bytes(),
				Updates: l.updates,
				Deletes: l.deletes,
				Children: children,
				Resume: l.resume,
			})
		}
	}
	return avro.Marshal(persistLayerSchema, persistLayers)
}

func removeInvalidLayers(m map[types.Hash]layer, h types.Hash) {
	layer, ok := m[h]
	if !ok { return }
	l, ok := layer.(*memoryLayer)
	if !ok { return }
	delete(m, h)
	for c := range l.children {
		removeInvalidLayers(m, c)
	}
}

func loadMap(m map[types.Hash]layer, persistenceData []byte, mut *sync.RWMutex) (types.Hash, error) {
	var persistLayers []memLayerPersist
	if err := avro.Unmarshal(persistLayerSchema, persistenceData, &persistLayers); err != nil {
		return types.Hash{}, err
	}
	parents := make(map[types.Hash]types.Hash)
	for _, pl := range persistLayers {
		if _, ok := m[pl.Hash]; ok {
			// This layer already exists in the map. (Parent layer?)
			continue
		}
		parents[pl.Hash] = pl.Parent
		l := &memoryLayer{
			hash: pl.Hash,
			parent: nil,
			num: uint64(pl.Number),
			blockWeight: new(big.Int).SetBytes(pl.Weight),
			updatesMap: make(map[string][]byte),
			updates: pl.Updates,
			deletesMap: make(map[string]struct{}),
			deletes: pl.Deletes,
			resume: pl.Resume,
			children: make(map[types.Hash]*memoryLayer),
			mut: mut,
		}
		for _, kv := range pl.Updates {
			l.updatesMap[string(kv.Key)] = kv.Value
		}
		for _, key := range pl.Deletes {
			l.deletesMap[string(key)] = struct{}{}
		}
		m[pl.Hash] = l
	}
	for childHash, parentHash := range parents {
		child := m[childHash]
		parent, ok := m[parentHash]
		if !ok { continue }
		switch l := parent.(type) {
		case *memoryLayer:
			l.children[childHash] = child.(*memoryLayer)
		case *diskLayer:
			l.children[childHash] = child.(*memoryLayer)
		}
		l := child.(*memoryLayer)
		l.parent = parent
	}
	for h, layer := range m {
		switch l := layer.(type) {
		case *memoryLayer:
			// Any memory layers with nil parents at this point are invalid, and they
			// should be removed along with their children
			if l.parent == nil {
				removeInvalidLayers(m, h)
			}
		default:
		}
	}
	var heaviestHash types.Hash
	heaviestWeight := new(big.Int)
	for h, l := range m {
		if blockWeight := l.weight(); blockWeight.Cmp(heaviestWeight) > 0 {
			heaviestWeight = blockWeight
			heaviestHash = h
		}
	}
	foundDisk := false
	for next := m[heaviestHash]; next != nil ; next = next.parentLayer() {
		if next.persisted() {
			foundDisk = true
			break
		}
	}
	for hash, l := range m {
		if _, ok := parents[hash]; ok {
			// Node is parent of something, so we'll get its depth elsewhere
			continue
		}
		depth := int64(0)
		for next := l; next != nil; next = next.parentLayer() {
			switch v := next.(type) {
			case *memoryLayer:
				if v.depth < depth {
					v.depth = depth
				}
			case *diskLayer:
				if v.depth < depth {
					v.depth = depth
				}
			}
			depth++
		}
	}
	if !foundDisk {
		return types.Hash{}, fmt.Errorf("persisted layer not in ancestry of heaviest hash")
	}
	return heaviestHash, nil
}
