package boltdb

import (
	"bytes"
	"fmt"
	"os"

	bolt "github.com/boltdb/bolt"
	dbpkg "github.com/openrelayxyz/cardinal-storage/db"
)

type Database struct {
	db *bolt.DB
}

func Open(path string, mode os.FileMode, options *bolt.Options) (*Database, error) {
	//if options are not specified default options will be set.
	fmt.Println("inside db open")
	db, err := bolt.Open(path, mode, options)
	return &Database{db: db}, err

}

func (db *Database) Update(fn func(dbpkg.Transaction) error) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("Cardinal"))
		return fn(&boltTx{
			bk:       b,
			reserves: make(map[string][]byte),
		})
	})
}

func (db *Database) View(fn func(dbpkg.Transaction) error) error {
	return db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Cardinal"))
		return fn(&boltTx{
			bk:       b,
			reserves: make(map[string][]byte),
		})
	})
}

func (db *Database) Close() {
	db.db.Close()
}

func (tx *boltTx) Get(key []byte) ([]byte, error) {
	item := tx.bk.Get(key)
	return item, nil
}

func (tx *boltTx) Put(key, value []byte) error {
	return tx.bk.Put(key, value)
}

func (tx *boltTx) PutReserve(key []byte, size int) ([]byte, error) {
	tx.reserves[string(key)] = make([]byte, size)
	return tx.reserves[string(key)], nil
}

func (tx *boltTx) Delete(key []byte) error {
	return tx.bk.Delete(key)
}

func (tx *boltTx) ZeroCopyGet(key []byte, fn func([]byte) error) error {
	val, err := tx.Get(key)
	if err != nil {
		return err
	}
	return fn(val)
}

func (tx *boltTx) Iterator(prefix []byte) dbpkg.Iterator {
	cursor := tx.bk.Cursor()
	key, val := cursor.Seek(prefix)
	it := &boltIterator{
		cursor:  cursor,
		first:   true,
		initKey: key,
		initVal: val,
		prefix:  prefix,
	}
	return it
}

type boltTx struct {
	bk       *bolt.Bucket
	reserves map[string][]byte
}

type boltIterator struct {
	cursor  *bolt.Cursor
	first   bool
	initKey []byte
	initVal []byte
	prefix  []byte
}

func (it *boltIterator) Next() bool {
	if !it.first {
		it.initKey, it.initVal = it.cursor.Next()
	} else {
		it.first = false
	}
	return it.initKey != nil && bytes.HasPrefix(it.initKey, it.prefix)
}

func (it *boltIterator) Key() []byte {
	return it.initKey
}

func (it *boltIterator) Value() []byte {
	return it.initVal
}

func (it *boltIterator) Error() error {
	return nil
}

func (it *boltIterator) Close() error {
	return nil
}
