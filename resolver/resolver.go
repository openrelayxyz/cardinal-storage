package resolver

import (
	"errors"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-storage"
	"github.com/openrelayxyz/cardinal-storage/current"
	"github.com/openrelayxyz/cardinal-storage/archive"
	dbpkg "github.com/openrelayxyz/cardinal-storage/db"
	"github.com/openrelayxyz/cardinal-storage/db/badgerdb"
	"github.com/openrelayxyz/cardinal-storage/db/boltdb"
	overlaypkg "github.com/openrelayxyz/cardinal-storage/db/overlay"
	"os"
	"strings"
	"net/url"
)

func ResolveStorage(path string, maxDepth int64, whitelist map[uint64]types.Hash) (storage.Storage, error) {
	var (
		db dbpkg.Database
		version []byte
		err error
	)
	if strings.HasPrefix(path, "overlay://") {
		parsedURL, err := url.Parse(path)
		if err != nil {
			return nil, err
		}
		// Extract paths from the "Host" and "Path" components
		// Since net/url treats everything after '//' as a host/path, manually split
		paths := strings.Split(parsedURL.Host+parsedURL.Path, ";")
		if len(paths) != 2 {
			return nil, errors.New("overlay path must have two semicolon delimited components")
		}

		// Extract the "cache" query parameter
		cache := (parsedURL.Query().Get("cache") == "1")

		underlay, v, err := resolveDatabase(paths[0])
		if err != nil {
			return nil, err
		}
		overlay, _, err := resolveDatabase(paths[1])
		if err != nil {
			return nil, err
		}
		db = overlaypkg.NewOverlayDatabase(underlay, overlay, cache)
		version = v
	
	} else {
		db, version, err = resolveDatabase(path)
		if err != nil {
			return nil, err
		}
	}
	switch string(version) {
	case "CurrentStorage1":
		return current.Open(db, maxDepth, whitelist)
	case "ArchiveStorage1":
		return archive.Open(db, maxDepth, whitelist)
	}
	return nil, storage.ErrUnknownStorageType
}

func resolveDatabase(path string) (dbpkg.Database, []byte, error) {
	fileInfo, err := os.Stat(path)
	var db dbpkg.Database
	if fileInfo.IsDir() {
		db, err = badgerdb.New(path)
	} else {
		db, err = boltdb.Open(path, 0600, nil)
	}
	if err != nil { return nil, nil,  err }
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
		return nil, nil, err
	}
	return db, version, nil
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
