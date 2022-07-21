package resolver

import (
	"github.com/openrelayxyz/cardinal-types"
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
			return nil
		} else {
			return err
		}
	}); err != nil {
		return nil, err
	}
	switch string(version) {
	case "CurrentStorage1":
		return current.Open(db, maxDepth, whitelist)
	case "ArchiveStorage1":
		return archive.Open(db, maxDepth, whitelist)
	}
	return nil, storage.ErrUnknownStorageType
}
func ResolveInitializer(path string, archival bool) (storage.Initializer, error) {
	var db dbpkg.Database
	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if fileInfo.IsDir() {
		db, err = badgerdb.New(path)
	} else {
		db, err = boltdb.Open(path, 0600, nil)
	}
	if err != nil { return nil, err }
	var version []byte
	if archival {
		version = []byte("ArchiveStorage1")
	} else {
		version = []byte("CurrentStorage1")
	}
	if err := db.Update(func(tx dbpkg.Transaction) error {
		v, err := tx.Get([]byte("CardinalStorageVersion"))
		if err == storage.ErrNotFound {
			tx.Put([]byte("CardinalStorageVersion"), version)
		} else if err == nil {
			version = v
		} else {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	switch string(version) {
	case "CurrentStorage1":
		return current.NewInitializer(db), nil
	case "ArchiveStorage1":
		return archive.NewInitializer(db), nil
	}
	return nil, storage.ErrUnknownStorageType
}
