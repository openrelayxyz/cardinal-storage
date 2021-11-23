package boltdb

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	dbpkg "github.com/openrelayxyz/cardinal-storage/db"
)

func TestUpdateTx(t *testing.T) {
	var db dbpkg.Database
	var err error
	db, err = Open("test0.db", 0600, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = db.Update(func(tx dbpkg.Transaction) error {
		fmt.Println("inside of update test")
		tx.Put([]byte("Hello"), []byte("World"))
		val, err := tx.Get([]byte("Hello"))
		if err != nil {
			return err
		}
		if !bytes.Equal(val, []byte("World")) {
			return errors.New("Unexpected value")
		}
		return nil
	})
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

func TestIterTx(t *testing.T) {
	var db dbpkg.Database
	var err error
	db, err = Open("test2.db", 0600, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	db.Update(func(tx dbpkg.Transaction) error {
		tx.Put([]byte("Hello"), []byte("World"))
		iter := tx.Iterator([]byte("H"))
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
		if iter.Next() {
			t.Errorf("Iterator should have been exhausted")
		}
		return nil
	})
}
