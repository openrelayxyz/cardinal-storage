package badgerdb

import (
	"bytes"
	"errors"
	"os"
	"testing"

	dbpkg "github.com/openrelayxyz/cardinal-storage/db"
)

var keys = [3]string{"Hello", "Yellow", "Mellow"}
var values = [3]string{"World", "Furled", "Burled"}

func TestUpdateTx(t *testing.T) {
	dir, er := os.MkdirTemp("", "testdir")
	if er != nil {
		t.Fatalf(er.Error())
	}
	path := dir + "/test.db"
	var db, rdb dbpkg.Database
	var err error
	defer os.RemoveAll(dir)
	db, err = New(path)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = db.Update(func(tx dbpkg.Transaction) error {
		for i := 0; i <= len(keys)-1; i++ {
			tx.Put([]byte(keys[i]), []byte(values[i]))
		}
		for i := 0; i <= len(keys)-1; i++ {
			val, err := tx.Get([]byte(keys[i]))
			if err != nil {
				return err
			}
			if !bytes.Equal(val, []byte(values[i])) {
				return errors.New("Unexpected value")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	db.Close()
	rdb, err = NewReadOnly(path)
	err = rdb.View(func(tx dbpkg.Transaction) error {
		if err := tx.Put([]byte("Goodbuy"), []byte("Horses")); err == nil {
			t.Errorf("Expected error calling Put() inside view tx")
		}
		for i := 0; i <= len(keys)-1; i++ {
			val, err := tx.Get([]byte(keys[i]))
			if err != nil {
				return err
			}
			if !bytes.Equal(val, []byte(values[i])) {
				return errors.New("Unexpected value")
			}
		}
		return nil

	})
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestIterTx(t *testing.T) {
	dir, er := os.MkdirTemp("", "testdir")
	if er != nil {
		t.Fatalf(er.Error())
	}
	path := dir + "/test.db"
	var db dbpkg.Database
	var err error
	db, err = New(path)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = db.Update(func(tx dbpkg.Transaction) error {
		for i := 0; i <= len(keys)-1; i++ {
			tx.Put([]byte(keys[i]), []byte(values[i]))
		}
		iter := tx.Iterator([]byte("H"))
		defer iter.Close()
		if !iter.Next() {
			t.Fatalf("Iterator should have had next item")
		}
		if string(iter.Key()) != "Hello" {
			t.Errorf("Unexpected key")
		}
		if string(iter.Value()) != "World" {
			t.Errorf("Unexpected value")
		}
		if iter.Next() {
			t.Errorf("Iterator should have been exhausted")
		}
		return nil
	})

	db.View(func(tx dbpkg.Transaction) error {
		iter := tx.Iterator([]byte("H"))
		defer iter.Close()
		if !iter.Next() {
			t.Fatalf("Iterator should have had next item")
		}
		if string(iter.Key()) != "Hello" {
			t.Errorf("Unexpected key")
		}
		if string(iter.Value()) != "World" {
			t.Errorf("Unexpected value")
		}
		if iter.Next() {
			t.Errorf("Iterator should have been exhausted")
		}
		return nil
	})
	db.View(func(tx dbpkg.Transaction) error {
		iter := tx.Iterator([]byte("Q"))
		defer iter.Close()
		if iter.Next() {
			t.Errorf("Iterator should have been exhausted")
		}
		return nil
	})
}
