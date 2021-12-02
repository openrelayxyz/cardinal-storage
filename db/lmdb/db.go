package lmdb

import (
	"bytes"

	lmdb "github.com/bmatsuo/lmdb-go/lmdb"
	dbpkg "github.com/openrelayxyz/cardinal-storage/db"
)

type Database struct {
	db *lmdb.Env
}

//todo: investigate the needs of cardinal to set the mapsize
func Open(path string) (*Database, error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	err = env.SetMaxDBs(1)
	if err != nil {
		return nil, err
	}
	err = env.SetMapSize(1 << 30)
	if err != nil {
		return nil, err
	}
	err = env.Open(path, 0, 0644)
	return &Database{db: env}, err
}

func (db *Database) Close() {
	db.db.Close()
}

type lmdbTx struct {
	dbi      lmdb.DBI
	tx       *lmdb.Txn
	reserves map[string][]byte
}

func (db *Database) Update(fn func(dbpkg.Transaction) error) error {
	return db.db.Update(func(txn *lmdb.Txn) error {
		dbi, err := txn.CreateDBI("Cardinal")
		if err != nil {
			return err
		}
		return fn(&lmdbTx{
			dbi:      dbi,
			tx:       txn,
			reserves: make(map[string][]byte),
		})
	})
}

func (db *Database) View(fn func(dbpkg.Transaction) error) error {
	return db.db.View(func(txn *lmdb.Txn) (err error) {
		if err != nil {
			return err
		}
		return nil
	})
	return nil
}

// func (db *Database) View(fn func(dbpkg.Transaction) error) error {
// 	return db.db.View(func(tx *bolt.Tx) error {
// 		b := tx.Bucket([]byte("Cardinal"))
// 		return fn(&boltTx{
// 			bk:       b,
// 			reserves: make(map[string][]byte),
// 		})
// 	})
// }

func (tx *lmdbTx) Get(key []byte) ([]byte, error) {
	value, err := tx.tx.Get(tx.dbi, key)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (tx *lmdbTx) ZeroCopyGet([]byte, func([]byte) error) error {
	return nil
}

func (tx *lmdbTx) Put(key, value []byte) error {
	return tx.tx.Put(tx.dbi, key, value, 0)
}

func (tx *lmdbTx) PutReserve([]byte, int) ([]byte, error) {
	return nil, nil
}

func (tx *lmdbTx) Delete([]byte) error {
	return nil
}

func (tx *lmdbTx) Iterator(prefix []byte) dbpkg.Iterator {
	cur, _ := tx.tx.OpenCursor(tx.dbi)
	key, value, _ := cur.Get(prefix, nil, 0)
	it := &lmIterator{
		cursor: cur,
		first:  true,
		key:    key,
		value:  value,
		prefix: prefix,
	}
	return it
	//return nil
}

// cursor := tx.bk.Cursor()
// key, val := cursor.Seek(prefix)
// it := &boltIterator{
// 	cursor:  cursor,
// 	first:   true,
// 	initKey: key,
// 	initVal: val,
// 	prefix:  prefix,
// }
// return it

type lmIterator struct {
	cursor *lmdb.Cursor
	first  bool
	key    []byte
	value  []byte
	prefix []byte
}

func (it *lmIterator) Next() bool {
	if !it.first {
		it.key, it.value, _ = it.cursor.Get(nil, nil, lmdb.Next)
	} else {
		it.first = false
	}
	return it.key != nil && bytes.HasPrefix(it.key, it.prefix)
}
func (it *lmIterator) Key() []byte {
	return it.key
}
func (it *lmIterator) Value() []byte {
	return it.value
}
func (it *lmIterator) Error() error {
	return nil
}
func (it *lmIterator) Close() error {
	return nil
}

// func (db *Database) Update(fn func(dbpkg.Transaction) error) error {
// 	return db.db.Update(func(tx *bolt.Tx) error {
// 		b, _ := tx.CreateBucketIfNotExists([]byte("Cardinal"))
// 		return fn(&boltTx{
// 			bk:       b,
// 			reserves: make(map[string][]byte),
// 		})
// 	})
// }
