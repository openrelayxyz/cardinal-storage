package badgerdb

import (
  "errors"
  "github.com/openrelayxyz/cardinal-storage"
  dbpkg "github.com/openrelayxyz/cardinal-storage/db"
  badger "github.com/dgraph-io/badger/v3"
)

type Database struct {
  db *badger.DB
}

func (db *Database) Close() {
  db.db.Close()
}

func New(path string) (*Database, error) {
  opt := badger.DefaultOptions(path)
  if path == "" {
    opt = opt.WithInMemory(true)
  }
  db, error := badger.Open(opt)
  return &Database{db: db}, error
}

type badgerTx struct {
  writable bool
  tx *badger.Txn
  reserves map[string][]byte
}

type badgerIterator struct {
  it *badger.Iterator
  prefix []byte
  item *badger.Item
  err error
}

func (it *badgerIterator) Next() bool {
  if it.err != nil { return false }
  it.it.Next()
  var ok bool
  if it.prefix != nil {
    ok = it.it.ValidForPrefix(it.prefix)
  } else {
    ok = it.it.Valid()
  }
  if ok { it.item = it.it.Item() }
  return ok
}
func (it *badgerIterator) Key() []byte {
  return it.item.Key()
}
func (it *badgerIterator) Value() []byte {
  val, err := it.item.ValueCopy(nil)
  if err != nil {
    it.err = err
  }
  return val
}
func (it *badgerIterator) Close() error {
  it.it.Close()
  return nil
}

func (it *badgerIterator) Error() error {
  return it.err
}

var (
  ErrReadOnly = errors.New("Attempt to write to read-only transaciton") // TODO: Move up a level
)

// Get returns a copy of the value at the specified key. The copy can continue
// to exist after the transaction closes, and may be manipulated without having
// problems.
func (tx *badgerTx) Get(key []byte) ([]byte, error) {
  item, err := tx.tx.Get(key)
  if err == badger.ErrKeyNotFound { return nil, storage.ErrNotFound }
  if err != nil { return nil, err }
  return item.ValueCopy(nil)
}

// ZeroCopyGet invokes a closure providing the value at the specified key. This
// value should be parsed and processed within the closure, as its memory may
// be reused soon after.
func (tx *badgerTx) ZeroCopyGet(key []byte, fn func([]byte) error) error {
  item, err := tx.tx.Get(key)
  if err == badger.ErrKeyNotFound { return storage.ErrNotFound }
  if err != nil { return err }
  return item.Value(func(value []byte) error {
    return fn(value)
  })
}

func (tx *badgerTx) Put(key, value []byte) (error) {
  if !tx.writable { return ErrReadOnly }
  return tx.tx.Set(key, value)
}

func (tx *badgerTx) PutReserve(key []byte, size int) ([]byte, error) {
  if !tx.writable { return nil, ErrReadOnly }
  tx.reserves[string(key)] = make([]byte, size)
  return tx.reserves[string(key)], nil
}

func (tx *badgerTx) Delete(key []byte) error {
  if !tx.writable { return ErrReadOnly }
  return tx.tx.Delete(key)
}

func (tx *badgerTx) Iterator(prefix []byte) dbpkg.Iterator {
  opts := badger.DefaultIteratorOptions
  opts.PrefetchSize = 10
  it := &badgerIterator{it: tx.tx.NewIterator(opts), prefix: prefix}
  if prefix != nil {
    it.it.Seek(prefix)
  }
  return it
}

func (db *Database) Update(fn func(dbpkg.Transaction) error) error {
  return db.db.Update(func(btx *badger.Txn) error {
    tx := &badgerTx{writable: true, tx: btx, reserves: make(map[string][]byte)}
    if err := fn(tx); err != nil { return err }
    for k, v := range tx.reserves {
      if err := tx.tx.Set([]byte(k), v); err != nil { return err }
    }
    return nil
  })
}
func (db *Database) View(fn func(dbpkg.Transaction) error) error {
  return db.db.View(func(btx *badger.Txn) error {
    tx := &badgerTx{writable: false, tx: btx}
    return fn(tx)
  })
}

// // Database allows the persistence and retrieval of key / value data.
// // Transaction allows for atomic interaction with the database. It can be used
// // to retrieve data or update the database, and a transaction should provide a
// // consistent view (changes made to the database by other transactions will not
// // effect the results returned by a transaction that was alread open)
// type Transaction interface {
//   // Get returns the value stored for a given key. Note that modifying the
//   // value returned here is unsafe, and transactions may return an error if
//   // this value is modified.
//   Get([]byte) ([]byte, error)
//   // Put sets a value at a specified key
//   Put([]byte, []byte) error
//   // PutReserve returns a byte slice of the specified size that can be updated
//   // to modify the specified key. For example, if you had a hexidecimal value
//   // and needed to store the decoded value, you could use PutReserve to get a
//   // byte slice and decode the hex into that byte slice, rather than decoding
//   // the hex into a new byte slice then calling Put(); this may be more
//   // efficient for some database implementations.
//   PutReserve([]byte, int) ([]byte, error)
//   // Delete removes a key from the database
//   Delete([]byte) error
//   // Iterator returns an iterator object that returns key / value pairs
//   // beginning with the specified prefix. Depending on the underlying database
//   // engine, the Iterator may or may not be ordered.
//   Iterator([]byte) Iterator
// }
//
// // Iterator iterates over key / value pairs beginning with the specified prefix.
// // Depending on the underlying database engine, the Iterator may or may not be
// // ordered.
// type Iterator interface {
//   // Next advances the iterator to the next key / value pair, returning True
//   // upon successful advancement, False if no key / value pairs remain. Next()
//   // should be called before accessing the first pair.
//   Next() bool
//   // Key returns the key associated with the current Key / Value pair
//   Key() []byte
//   // Value returns the value associated with the current Key / Value pair. Note
//   // that depending on the database implementation, accessing values may be
//   // considerably more expensive than just accessing keys.
//   Value() []byte
//   Close() error
// }
//
//
// // See for implementing iterators in lmdb:
// // https://pkg.go.dev/github.com/bmatsuo/lmdb-go@v1.8.0/lmdbscan#New
