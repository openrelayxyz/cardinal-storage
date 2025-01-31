package overlay

import (
	"bytes"
	"errors"
	dbpkg "github.com/openrelayxyz/cardinal-storage/db"
	"github.com/openrelayxyz/cardinal-storage/db/mem"
	"testing"
)

func TestUpdateTx(t *testing.T) {
	underlay := mem.NewMemoryDatabase(1024)
	err := underlay.Update(func(tx dbpkg.Transaction) error {
		return tx.Put([]byte("Hello"), []byte("world?"))
	})
	overlay := mem.NewMemoryDatabase(1024)
	db := NewOverlayDatabase(underlay, overlay, true)
	err = db.View(func(tx dbpkg.Transaction) error {
		val, err := tx.Get([]byte("Hello"))
		if err != nil {
			t.Fatalf(err.Error())
		}
		if !bytes.Equal(val, []byte("world?")) {
			t.Errorf("Unexpected value: %v", string(val))
		}
		return nil
	})
	if err != nil {
		t.Errorf(err.Error())
	}
	err = db.Update(func(tx dbpkg.Transaction) error {
		tx.Put([]byte("Hello"), []byte("World!"))
		val, err := tx.Get([]byte("Hello"))
		if err != nil {
			return err
		}
		if !bytes.Equal(val, []byte("World!")) {
			return errors.New("Unexpected value")
		}
		return nil
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = db.View(func(tx dbpkg.Transaction) error {
		if err := tx.Put([]byte("Hello"), []byte("Person")); err == nil {
			t.Errorf("Expected error calling Put() inside view tx")
		}
		val, err := tx.Get([]byte("Hello"))
		if err != nil {
			t.Fatalf(err.Error())
		}
		if !bytes.Equal(val, []byte("World!")) {
			t.Errorf("Unexpected value: %v", string(val))
		}
		return nil
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
}
func TestBw(t *testing.T) {
	underlay := mem.NewMemoryDatabase(1024)
	err := underlay.Update(func(tx dbpkg.Transaction) error {
		return tx.Put([]byte("Hello"), []byte("world?"))
	})
	overlay := mem.NewMemoryDatabase(1024)
	db := NewOverlayDatabase(underlay, overlay, true)
	bw := db.BatchWriter()
	err = bw.Put([]byte("Hello"), []byte("World"))
	bw.Flush()
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = db.View(func(tx dbpkg.Transaction) error {
		if err := tx.Put([]byte("Hello"), []byte("World")); err == nil {
			t.Errorf("Expected error calling Put() inside view tx")
		}
		val, err := tx.Get([]byte("Hello"))
		if err != nil {
			t.Fatalf(err.Error())
		}
		if !bytes.Equal(val, []byte("World")) {
			t.Errorf("Unexpected value: %v", string(val))
		}
		return nil
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
}
func TestAlterGetTx(t *testing.T) {
	underlay := mem.NewMemoryDatabase(1024)
	overlay := mem.NewMemoryDatabase(1024)
	db := NewOverlayDatabase(underlay, overlay, true)
	err := db.Update(func(tx dbpkg.Transaction) error {
		tx.Put([]byte("Hello"), []byte("World"))
		val, err := tx.Get([]byte("Hello"))
		if err != nil {
			return err
		}
		if !bytes.Equal(val, []byte("World")) {
			return errors.New("Unexpected value")
		}
		val[0] = 3
		return nil
	})
	if err == nil {
		t.Fatalf("Expected failure")
	}
}
func TestCacheHit(t *testing.T) {
	underlay := mem.NewMemoryDatabase(1024)
	err := underlay.Update(func(tx dbpkg.Transaction) error {
		return tx.Put([]byte("Hello"), []byte("World"))
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	hits := overlayHitMeter.Count()
	overlay := mem.NewMemoryDatabase(1024)
	db := NewOverlayDatabase(underlay, overlay, true)
	db.View(func(tx dbpkg.Transaction) error {
		_, err := tx.Get([]byte("Hello"))
		if err != nil {
			return err
		}
		return nil
	})
	if overlayHitMeter.Count() != hits {
		t.Errorf("Unexpected hit")
	}
	db.View(func(tx dbpkg.Transaction) error {
		_, err := tx.Get([]byte("Hello"))
		if err != nil {
			return err
		}
		return nil
	})
	if overlayHitMeter.Count() <= hits {
		t.Errorf("Expected hit")
	}
}
func TestIterTx(t *testing.T) {
	underlay := mem.NewMemoryDatabase(1024)
	overlay := mem.NewMemoryDatabase(1024)
	db := NewOverlayDatabase(underlay, overlay, true)
	db.Update(func(tx dbpkg.Transaction) error {
		tx.Put([]byte("Hello"), []byte("World"))
		iter := tx.Iterator([]byte("H"))
		if !iter.Next() {
			t.Errorf("Iterator should have had next item")
		}
		if string(iter.Key()) != "Hello" {
			t.Errorf("Unexpected key '%v'", string(iter.Key()))
		}
		if string(iter.Value()) != "World" {
			t.Errorf("Unexpected value '%v'", string(iter.Value()))
		}
		if iter.Next() {
			t.Errorf("Iterator should have been exhausted")
		}
		return nil
	})
	db.View(func(tx dbpkg.Transaction) error {
		iter := tx.Iterator([]byte("H"))
		if !iter.Next() {
			t.Errorf("Iterator should have had next item")
		}
		if string(iter.Key()) != "Hello" {
			t.Errorf("Unexpected key '%v'", string(iter.Key()))
		}
		if string(iter.Value()) != "World" {
			t.Errorf("Unexpected value '%v'", string(iter.Value()))
		}
		if iter.Next() {
			t.Errorf("Iterator should have been exhausted")
		}
		return nil
	})
	db.View(func(tx dbpkg.Transaction) error {
		iter := tx.Iterator([]byte("Q"))
		if iter.Next() {
			t.Errorf("Iterator should have been exhausted")
		}
		return nil
	})
}
