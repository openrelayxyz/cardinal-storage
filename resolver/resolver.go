package resolver

import (
	"github.com/openrelayxyz/cardinal-storage"
	"github.com/openrelayxyz/cardinal-storage/current"
	"github.com/openrelayxyz/cardinal-storage/archive"
	dbpkg "github.com/openrelayxyz/cardinal-storage/db"
	"github.com/openrelayxyz/cardinal-storage/db/badgerdb"
	"github.com/openrelayxyz/cardinal-storage/db/boltdb"
	"os"
)

func ResolveStorage(path string, maxDepth int64, whitelist map[uint64]types.Hash) (storage.Storage, error) {
	fileInfo, err := os.Stat(path)
	var db dbpkg.Database
	var err error
	if fileInfo.IsDir() {
		db, err = badgerdb.New(path)
	} else {
		db, err = boltdb.Open(path, 0600, nil)
	}
	if err != nil { return nil, err }
	var version []byte
	if err := db.Update(func(tx dbpkg.Transaction) error {
		version, err = tx.Get([]byte("CardinalStorageVersion"))
		if err == storage.ErrNotFound {
			version = []byte("CurrentStorage1")
			tx.Put([]byte("CardinalStorageVersion"), []byte("CurrentStorage1"))
		} else {
			return err
		}
	}); err != nil {
		return err
	}
	switch string(version) {
	case "CurrentStorage1":
		return current.Open(db, maxDepth, whitelist)
	case "ArchiveStorage1":
		return archive.Open(db, maxDepth, whitelist)
	}
	return nil, storage.ErrUnknownStorageType
}