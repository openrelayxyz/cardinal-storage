package overlay

import (
	"bytes"
	"sync/atomic"
	"github.com/openrelayxyz/cardinal-storage"
	"github.com/openrelayxyz/cardinal-types/metrics"
	dbpkg "github.com/openrelayxyz/cardinal-storage/db"
)

var (
	overlayHitMeter  = metrics.NewMinorMeter("/storage/overlay/hit")
	overlayMissMeter  = metrics.NewMinorMeter("/storage/overlay/miss")
	overlayCacheHistogram = metrics.NewMinorHistogram("/storage/overlay/txhit")
)

// Database: The overlay database will return values from the overlay if
// present, the underlay if not. If `cache` is true, it will cache values
// retrieved from the underlay into the overlay (caching should only be used
// if the overlay is expected to have significantly better performance than
// the underlay).
type Database struct {
	overlay  dbpkg.Database
	underlay dbpkg.Database
	cache    bool
}

func NewOverlayDatabase(underlay, overlay dbpkg.Database, cache bool) dbpkg.Database {
	return &Database{
		overlay: overlay,
		underlay: underlay,
		cache: cache,
	}
}

// View invokes a closure, providing a read-only transaction.
func (db *Database) View(fn func(dbpkg.Transaction) error) error {
	return db.overlay.Update(func(otx dbpkg.Transaction) error {
		return db.underlay.View(func(utx dbpkg.Transaction) error {
			tx := &overlayTransaction{otx, utx, db.cache, true, new(int64), new(int64)}
			defer tx.writeMetrics()
			return fn(tx)
		})
	})
}

// Update invokes a closure, providing a read/write transaction
func (db *Database) Update(fn func(dbpkg.Transaction) error) error {
	return db.overlay.Update(func(otx dbpkg.Transaction) error {
		return db.underlay.View(func(utx dbpkg.Transaction) error {
			tx := &overlayTransaction{otx, utx, db.cache, false, new(int64), new(int64)}
			defer tx.writeMetrics()
			return fn(tx)
		})
	})
}

func (db *Database) BatchWriter() dbpkg.BatchWriter {
	return db.overlay.BatchWriter()
}

func (db *Database) Close() {
	db.underlay.Close()
	db.overlay.Close()
}

func (db *Database) Vacuum() bool {
	return db.underlay.Vacuum() || db.overlay.Vacuum()
}

type overlayTransaction struct {
	overlaytx  dbpkg.Transaction
	underlaytx dbpkg.Transaction
	cache      bool
	readonly   bool
	hits       *int64
	misses     *int64
}

var (
	deletePrefix = []byte("DELETED/")
)

func (tx *overlayTransaction) writeMetrics() {
	denominator := *tx.hits + *tx.misses
	if denominator == 0 {
		overlayCacheHistogram.Update(0)
	} else {
		overlayCacheHistogram.Update(100 * *tx.hits / (*tx.hits + *tx.misses))
	}
}

func (tx *overlayTransaction) isDeleted(key []byte) bool {
	_, err := tx.overlaytx.Get(append(deletePrefix, key...))
	return err != storage.ErrNotFound
}
func (tx *overlayTransaction) Get(key []byte) ([]byte, error) {
	if v, err := tx.overlaytx.Get(key); err == nil {
		atomic.AddInt64(tx.hits, 1)
		overlayHitMeter.Mark(1)
		return v, err
	}
	if tx.isDeleted(key) {
		atomic.AddInt64(tx.hits, 1)
		overlayHitMeter.Mark(1)
		return nil, storage.ErrNotFound
	}
	atomic.AddInt64(tx.misses, 1)
	overlayMissMeter.Mark(1)
	v, err := tx.underlaytx.Get(key)
	if err == nil && tx.cache {
		tx.overlaytx.Put(key, v)
	}
	return v, err
}

func (tx *overlayTransaction) ZeroCopyGet(key []byte, fn func([]byte) error) error {
	if err := tx.overlaytx.ZeroCopyGet(key, fn); err == nil {
		atomic.AddInt64(tx.hits, 1)
		overlayHitMeter.Mark(1)
		return err
	}
	if tx.isDeleted(key) {
		atomic.AddInt64(tx.hits, 1)
		overlayHitMeter.Mark(1)
		return storage.ErrNotFound
	}
	atomic.AddInt64(tx.misses, 1)
	overlayMissMeter.Mark(1)
	return tx.underlaytx.ZeroCopyGet(key, fn)
}

func (tx *overlayTransaction) Put(key, value []byte) error {
	if tx.readonly { return storage.ErrWriteToReadOnly }
	tx.overlaytx.Delete(append(deletePrefix, key...))
	return tx.overlaytx.Put(key, value)
}

func (tx *overlayTransaction) PutReserve(key []byte, size int) ([]byte, error) {
	if tx.readonly { return nil, storage.ErrWriteToReadOnly }
	tx.overlaytx.Delete(append(deletePrefix, key...))
	return tx.overlaytx.PutReserve(key, size)
}

func (tx *overlayTransaction) Delete(key []byte) error {
	if tx.readonly { return storage.ErrWriteToReadOnly }
	err := tx.overlaytx.Delete(key)
	tx.overlaytx.Put(append(deletePrefix, key...), []byte{0})
	return err
}

func (tx *overlayTransaction) Iterator(prefix []byte) dbpkg.Iterator {
	oiter := tx.overlaytx.Iterator(prefix)
	uiter := tx.underlaytx.Iterator(prefix)
	odone := !oiter.Next()
	udone := !uiter.Next()
	return &overlayIterator{
		tx:    tx,
		oiter: oiter,
		uiter: uiter,
		odone: odone,
		udone: udone,
	}
}

type overlayIterator struct {
	tx           *overlayTransaction
	oiter, uiter dbpkg.Iterator
	odone, udone bool
	key, val     []byte
	err          error
}

func (wi *overlayIterator) Next() bool {
	// If both are done and there is no error, the iterator should return false
	if (wi.odone && wi.udone) || wi.err != nil {
		return false
	}
	// Skip past any DELETE/ prefixed keys in the overlay to find the next overlay key
	oKey := wi.oiter.Key()
	for !wi.odone && bytes.HasPrefix(oKey, deletePrefix) {
		if !wi.oiter.Next() {
			// We've exhausted the overlay iterator, mark it as done
			wi.odone = true
			wi.err = wi.oiter.Error()
		}
		oKey = wi.oiter.Key()
	}
	// Skip past any DELETE/ prefixed keys in the underlay to find the next underlay key
	uKey := wi.uiter.Key()
	for !wi.udone {
		if !wi.tx.isDeleted(uKey) {
			break
		}
		if !wi.uiter.Next() {
			// We've exhausted the underlay iterator, mark it as done
			wi.udone = true
			wi.err = wi.uiter.Error()
		}
	}
	// The overlay is not done. If the underlay is done or the overlay's key is ahead of the underlay's key,
	// set key and value to the values from the overlay
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
	if oerr != nil {
		return oerr
	}
	return uerr
}
