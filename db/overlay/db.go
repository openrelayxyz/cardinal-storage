package overlay

import (
	"bytes"
	"github.com/openrelayxyz/cardinal-storage"
	dbpkg "github.com/openrelayxyz/cardinal-storage/db"
)


// Database: The overlay database will return values from the overlay if
// present, the underlay if not. If `cache` is true, it will cache values
// retrieved from the underlay into the overlay (caching should only be used
// if the overlay is expected to have significantly better performance than
// the underlay).
type Database struct{
	overlay  dbpkg.Database
	underlay dbpkg.Database
	cache    bool
}

// View invokes a closure, providing a read-only transaction.
func (db *Database) View(fn func(dbpkg.Transaction) error) error {
	return db.overlay.View(func(otx dbpkg.Transaction) error {
		return db.underlay.View(func(utx dbpkg.Transaction) error {
			return fn(&overlayTransaction{otx, utx, db.cache})
		})
	})
}

// Update invokes a closure, providing a read/write transaction
func (db *Database) Update(fn func(dbpkg.Transaction) error) error {
	return db.overlay.Update(func(otx dbpkg.Transaction) error {
		return db.underlay.Update(func(utx dbpkg.Transaction) error {
			return fn(&overlayTransaction{otx, utx, db.cache})
		})
	})
}

type overlayTransaction struct {
	overlaytx  dbpkg.Transaction
	underlaytx dbpkg.Transaction
	cache      bool
}

var (
	deletePrefix = []byte("DELETED/")
)

func (tx *overlayTransaction) isDeleted(key []byte) bool {
	_, err := tx.overlaytx.Get(append(deletePrefix, key...))
	return err != storage.ErrNotFound
}

func (tx *overlayTransaction) Get(key []byte) ([]byte, error) {
	if v, err := tx.overlaytx.Get(key); err == nil {
		return v, err
	}
	if tx.isDeleted(key) {
		return nil, storage.ErrNotFound
	}
	v, err := tx.underlaytx.Get(key)
	if err == nil && tx.cache {
		tx.overlaytx.Put(key, v)
	}
	return v, err
}

func (tx *overlayTransaction) ZeroCopyGet(key []byte, fn func([]byte) error) (error) {
	if err := tx.overlaytx.ZeroCopyGet(key, fn); err == nil {
		return err
	}
	if tx.isDeleted(key) {
		return storage.ErrNotFound
	}
	return tx.underlaytx.ZeroCopyGet(key, fn)
}

func (tx *overlayTransaction) Put(key, value []byte) error {
	tx.overlaytx.Delete(append(deletePrefix, key...))
	return tx.overlaytx.Put(key, value)
}

func (tx *overlayTransaction) PutReserve(key []byte, size int) ([]byte, error) {
	tx.overlaytx.Delete(append(deletePrefix, key...))
	return tx.overlaytx.PutReserve(key, size)
}

func (tx *overlayTransaction) Delete(key []byte) error {
	err := tx.overlaytx.Delete(key)
	tx.overlaytx.Put(append(deletePrefix, key...), []byte{0})
	return err
}

func (tx *overlayTransaction) Iterator(prefix []byte) dbpkg.Iterator {
	return &overlayIterator{
		tx: tx,
		oiter: tx.overlaytx.Iterator(prefix),
		uiter: tx.overlaytx.Iterator(prefix),
	}
}

type overlayIterator struct{
	tx    *overlayTransaction
	oiter, uiter dbpkg.Iterator
	odone, udone bool
	key, val []byte
	err   error
}

func (wi *overlayIterator) Next() bool {
	if (wi.odone && wi.udone) || wi.err != nil {
		return false
	}
	oKey := wi.oiter.Key()
	for !wi.odone && bytes.HasPrefix(oKey, deletePrefix) {
		if !wi.oiter.Next() {
			wi.odone = true
			wi.err = wi.oiter.Error()
		}
		oKey = wi.oiter.Key()
	}
	uKey := wi.uiter.Key()
	for !wi.udone {
		if wi.tx.isDeleted(uKey) {
			break
		}
		if !wi.uiter.Next() {
			wi.udone = true
			wi.err = wi.uiter.Error()
		}
	}
	if !wi.odone {
		if wi.udone || (bytes.Compare(oKey, uKey) < 0) {
			wi.key = oKey
			wi.val = wi.oiter.Value()
			if !wi.oiter.Next() {
				wi.odone = true
				wi.err = wi.oiter.Error()
			}
			return true
		}
	}
	if !wi.udone {
		wi.key = uKey
		wi.val = wi.uiter.Value()
		if !wi.uiter.Next() {
			wi.udone = true
			wi.err = wi.uiter.Error()
		}
		return true
	}
	return false
}

func (wi *overlayIterator) Key() []byte {
	return wi.key
}
func (wi *overlayIterator) Value() []byte {
	return wi.val
}
func (wi *overlayIterator) Error() error {
	return wi.err
}
func (wi *overlayIterator) Close() error {
	oerr := wi.oiter.Close()
	uerr := wi.uiter.Close()
	if oerr != nil { return oerr }
	return uerr
}
