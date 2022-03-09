package mem

import (
  "bytes"
  "sync"
  "errors"
  "fmt"
  "testing"
  dbpkg "github.com/openrelayxyz/cardinal-storage/db"
  "github.com/openrelayxyz/cardinal-storage"
)

type Database struct {
  data map[string][]byte
  locker *sync.RWMutex
  wlocker *sync.Mutex
  semaphore chan struct{}
  t *testing.T
}

func NewMemoryDatabase(concurrentReaders int) *Database {
  return &Database{
    data: make(map[string][]byte),
    locker: &sync.RWMutex{},
    wlocker: &sync.Mutex{},
    semaphore: make(chan struct{}, concurrentReaders),
  }
}

func fnWrapper(fn func(tx dbpkg.Transaction) error) func(tx dbpkg.Transaction) error {
  return func(tx dbpkg.Transaction) error {
    if err := fn(tx); err != nil { return err }
    switch txt := tx.(type) {
    case *fullTransaction:
      for k, v := range txt.origValues {
        if !bytes.Equal(txt.returnedValues[k], v) {
          return fmt.Errorf("Value changed during transaction: %x != %x", v, txt.returnedValues[k])
        }
      }
    case *viewTransaction:
      for k, v := range txt.origValues {
        if !bytes.Equal(txt.returnedValues[k], v) {
          return fmt.Errorf("Value changed during transaction: %x != %x", v, txt.returnedValues[k])
        }
      }
    default:
    }
    return nil
  }
}

func (db *Database) View(fn func(tx dbpkg.Transaction) error) error {
  db.semaphore <- struct{}{}
  defer func() {<-db.semaphore}()
  db.locker.RLock()
  defer db.locker.RUnlock()
  return fnWrapper(fn)(&viewTransaction{
    db: db,
    origValues: make(map[string][]byte),
    returnedValues: make(map[string][]byte),
  })
}

func (db *Database) Update(fn func(tx dbpkg.Transaction) error) error {
  if fn == nil { return errors.New("No function provided")}
  db.wlocker.Lock()
  defer db.wlocker.Unlock()
  db.semaphore <- struct{}{}
  defer func() {<-db.semaphore}()
  tx := &fullTransaction{
    db: db,
    changes: make(map[string][]byte),
    deletes: make(map[string]struct{}),
    origValues: make(map[string][]byte),
    returnedValues: make(map[string][]byte),
  }
  if err := fnWrapper(fn)(tx); err != nil { return err }
  db.locker.Lock()
  defer db.locker.Unlock()
  return tx.apply(db.data)
}

func (db *Database) Close() {}

func (db *Database) Vacuum() bool {
	return false
}

type viewTransaction struct {
  db *Database
  origValues map[string][]byte
  returnedValues map[string][]byte
}

type memIterator struct {
  tx dbpkg.Transaction
  keyCh chan []byte
  key []byte
  quit chan struct{}
}

func (tx *viewTransaction) Get(key []byte) ([]byte, error) {
  if val, ok := tx.db.data[string(key)]; ok {
    tx.returnedValues[string(key)] = val
    tx.origValues[string(key)] = make([]byte, len(val))
    copy(tx.origValues[string(key)][:], val)
    return val, nil
  }
  return []byte{}, storage.ErrNotFound
}

func (tx *viewTransaction) ZeroCopyGet(key []byte, fn func([]byte) error) error {
  val, err := tx.Get(key)
  if err != nil { return err }
  return fn(val)
}

func (tx *viewTransaction) Put([]byte, []byte) error {
  return storage.ErrWriteToReadOnly
}

func (tx *viewTransaction) PutReserve([]byte, int) ([]byte, error) {
  return []byte{}, storage.ErrWriteToReadOnly
}

func (tx *viewTransaction) Delete([]byte) error {
  return storage.ErrWriteToReadOnly
}

func (tx *viewTransaction) Iterator(prefix []byte) dbpkg.Iterator {
  iter := &memIterator{
    tx: tx,
    keyCh: make(chan []byte),
    quit: make(chan struct{}),
  }
  go func() {
    for k := range tx.db.data {
      kb := []byte(k)
      if bytes.HasPrefix(kb, prefix) {
        select {
        case iter.keyCh <- kb:
        case <-iter.quit:
          close(iter.keyCh)
          return
        }
      }
    }
    close(iter.keyCh)
  }()
  return iter
}

func (iter *memIterator) Next() bool {
  var ok bool
  iter.key, ok = <-iter.keyCh
  return ok
}

func (iter *memIterator) Key() []byte {
  return iter.key
}

func (iter *memIterator) Value() []byte {
  k, _ := iter.tx.Get(iter.key)
  return k
}

func (iter *memIterator) Close() error {
  return nil
}

func (iter *memIterator) Error() error {
  return nil
}


type fullTransaction struct{
  db *Database
  changes map[string][]byte
  deletes map[string]struct{}
  origValues map[string][]byte
  returnedValues map[string][]byte
}

func (tx *fullTransaction) ZeroCopyGet(key []byte, fn func([]byte) error) error {
  val, err := tx.Get(key)
  if err != nil { return err }
  return fn(val)
}


func (tx *fullTransaction) Get(key []byte) ([]byte, error) {
  if _, ok := tx.deletes[string(key)]; ok {
    return []byte{}, storage.ErrNotFound
  }
  if val, ok := tx.changes[string(key)]; ok {
    tx.returnedValues[string(key)] = val
    tx.origValues[string(key)] = make([]byte, len(val))
    copy(tx.origValues[string(key)][:], val)
    return val, nil
  }
  if val, ok := tx.db.data[string(key)]; ok {
    tx.returnedValues[string(key)] = val
    tx.origValues[string(key)] = make([]byte, len(val))
    copy(tx.origValues[string(key)][:], val)
    return val, nil
  }
  return []byte{}, storage.ErrNotFound
}

func (tx *fullTransaction) Put(key []byte, val []byte) error {
  delete(tx.deletes, string(key))
  tx.changes[string(key)] = val
  return nil
}

func (tx *fullTransaction) PutReserve(key []byte, n int) ([]byte, error) {
  delete(tx.deletes, string(key))
  tx.changes[string(key)] = make([]byte, n)
  return tx.changes[string(key)], nil
}

func (tx *fullTransaction) Delete(key []byte) error {
  delete(tx.changes, string(key))
  tx.deletes[string(key)] = struct{}{}
  return nil
}

func (tx *fullTransaction) Iterator(prefix []byte) dbpkg.Iterator {
  iter := &memIterator{
    tx: tx,
    keyCh: make(chan []byte),
  }
  go func() {
    for k := range tx.changes {
      kb := []byte(k)
      if bytes.HasPrefix(kb, prefix) {
        iter.keyCh <- kb
      }
    }
    for k := range tx.db.data {
      if _, ok := tx.changes[k]; ok { continue }
      if _, ok := tx.deletes[k]; ok { continue }
      kb := []byte(k)
      if bytes.HasPrefix(kb, prefix) {
        iter.keyCh <- kb
      }
    }
    close(iter.keyCh)
  }()
  return iter
}

func (tx *fullTransaction) apply(data map[string][]byte) error {
  for k := range tx.deletes {
    delete(data, k)
  }
  for k, v := range tx.changes {
    data[k] = v
  }
  return nil
}
