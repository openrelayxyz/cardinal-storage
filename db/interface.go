package db


// Database allows the persistence and retrieval of key / value data.
type Database interface {
  // View provides a Transaction that can be used to read from the database
  View(func(Transaction) error) error
  // Update provides a Transaction tht can be used to read from or write to the database
  Update(func(Transaction) error) error
  // Vacuum takes time to clean up the on-disk representation of data
  // (compaction or vacuuming, depending on storage engine). Returns true if
  // any progress was made (running again after a false will have no effect).
  // This may result in an optimally compact database, and multiple runs may be
  // necessary
  Vacuum() bool
  Close()
}

// Transaction allows for atomic interaction with the database. It can be used
// to retrieve data or update the database, and a transaction should provide a
// consistent view (changes made to the database by other transactions will not
// effect the results returned by a transaction that was alread open)
type Transaction interface {
  // Get returns the value stored for a given key. Note that modifying the
  // value returned here is unsafe, and transactions may return an error if
  // this value is modified.
  Get([]byte) ([]byte, error)
  // ZeroCopyGet invokes a closure, providing the value stored at the specified
  // key. This value must only be accessed within the closure, and the slice
  // may be modified after the closure finishes executing. The data must be
  // parsed and / or copied within the closure.
  ZeroCopyGet([]byte, func([]byte) error) error
  // Put sets a value at a specified key
  Put([]byte, []byte) error
  // PutReserve returns a byte slice of the specified size that can be updated
  // to modify the specified key. For example, if you had a hexidecimal value
  // and needed to store the decoded value, you could use PutReserve to get a
  // byte slice and decode the hex into that byte slice, rather than decoding
  // the hex into a new byte slice then calling Put(); this may be more
  // efficient for some database implementations.
  PutReserve([]byte, int) ([]byte, error)
  // Delete removes a key from the database
  Delete([]byte) error
  // Iterator returns an iterator object that returns key / value pairs
  // beginning with the specified prefix. Depending on the underlying database
  // engine, the Iterator may or may not be ordered.
  Iterator([]byte) Iterator
}

// Iterator iterates over key / value pairs beginning with the specified prefix.
// Depending on the underlying database engine, the Iterator may or may not be
// ordered.
type Iterator interface {
  // Next advances the iterator to the next key / value pair, returning True
  // upon successful advancement, False if no key / value pairs remain. Next()
  // should be called before accessing the first pair.
  Next() bool
  // Key returns the key associated with the current Key / Value pair
  Key() []byte
  // Value returns the value associated with the current Key / Value pair. Note
  // that depending on the database implementation, accessing values may be
  // considerably more expensive than just accessing keys.
  Value() []byte
  Error() error
  Close() error
}


// See for implementing iterators in lmdb:
// https://pkg.go.dev/github.com/bmatsuo/lmdb-go@v1.8.0/lmdbscan#New
