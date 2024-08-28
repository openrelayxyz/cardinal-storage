package current

import (
	// "errors"
	// "bytes"
	"time"
	"github.com/openrelayxyz/cardinal-storage"
	"github.com/openrelayxyz/cardinal-storage/db/mem"
	"github.com/openrelayxyz/cardinal-types"
	"math/big"
	"testing"
	"sync"
)

type testWaiter struct {
	hashes map[types.Hash]chan struct{}
	number map[int64]chan struct{}
	t *testing.T
}

func (w *testWaiter) WaitForHash(h types.Hash, d time.Duration) {
	if _, ok := w.hashes[h]; !ok {
		w.hashes[h] = make(chan struct{})
	}
	select {
	case <-w.hashes[h]:
	case <-time.After(d):
		w.t.Errorf("Unexpected timeout waiting for hash")
	}
}
func (w *testWaiter) WaitForNumber(n int64, d time.Duration) {
	if _, ok := w.number[n]; !ok {
		w.number[n] = make(chan struct{})
	}
	select {
	case <-w.number[n]:
	case <-time.After(d):
		w.t.Errorf("Unexpected timeout waiting for number")
	}
}
func (w *testWaiter) release(h types.Hash, n int64) {
	if ch, ok := w.hashes[h]; ok {
		close(ch)
	}
	if ch, ok := w.number[n]; ok {
		close(ch)
	}
}

func newTestWaiter(t *testing.T) *testWaiter {
	return &testWaiter{
		hashes: make(map[types.Hash]chan struct{}),
		number: make(map[int64]chan struct{}),
		t: t,
	}
}

func TestAddBlock(t *testing.T) {
	db := mem.NewMemoryDatabase(1024)
	s := New(db, 8, nil)
	s.RegisterWaiter(newTestWaiter(t), 250*time.Millisecond)
	if err := s.AddBlock(
		types.HexToHash("a"),
		types.Hash{},
		1,
		big.NewInt(1),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("/a/Data"), Value: []byte("Something")},
			storage.KeyValue{Key: []byte("/a/Data2"), Value: []byte("Something Else")},
			storage.KeyValue{Key: []byte("a"), Value: []byte("1")},
			storage.KeyValue{Key: []byte("b"), Value: []byte("2")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.AddBlock(
		types.HexToHash("b"),
		types.HexToHash("a"),
		2,
		big.NewInt(2),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("/b/Data"), Value: []byte("Something 1")},
			storage.KeyValue{Key: []byte("/b/Data2"), Value: []byte("Something Else 1")},
			storage.KeyValue{Key: []byte("a"), Value: []byte("3")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}

	if err := s.View(types.HexToHash("a"), func(tr storage.Transaction) error {
		if data, err := tr.Get([]byte("a")); err != nil {
			return err
		} else if string(data) != "1" {
			t.Errorf("Unexpected value for a")
		}
		if data, err := tr.Get([]byte("b")); err != nil {
			return err
		} else if string(data) != "2" {
			t.Errorf("Unexpected value for b")
		}
		return nil
	}); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.View(types.HexToHash("b"), func(tr storage.Transaction) error {
		if data, err := tr.Get([]byte("a")); err != nil {
			return err
		} else if string(data) != "3" {
			t.Errorf("Unexpected value for a")
		}
		if data, err := tr.Get([]byte("b")); err != nil {
			return err
		} else if string(data) != "2" {
			t.Errorf("Unexpected value for b")
		}
		if data, err := tr.Get([]byte("/a/Data")); err != nil {
			return err
		} else if string(data) != "Something" {
			t.Errorf("Unexpected value for a.Data")
		}
		if data, err := tr.Get([]byte("/b/Data")); err != nil {
			return err
		} else if string(data) != "Something 1" {
			t.Errorf("Unexpected value for a.Data")
		}
		return nil
	}); err != nil {
		t.Errorf(err.Error())
	}
	if latest := s.LatestHash(); latest != types.HexToHash("b") {
		t.Errorf("expected latest hash to be 'b', got %#x", latest)
	}
}
func TestFork(t *testing.T) {
	db := mem.NewMemoryDatabase(1024)
	s := New(db, 8, nil)
	s.RegisterWaiter(newTestWaiter(t), 250*time.Millisecond)
	if err := s.AddBlock(
		types.HexToHash("a"),
		types.Hash{},
		1,
		big.NewInt(1),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("/a/Data"), Value: []byte("Something")},
			storage.KeyValue{Key: []byte("/a/Data2"), Value: []byte("Something Else")},
			storage.KeyValue{Key: []byte("a"), Value: []byte("1")},
			storage.KeyValue{Key: []byte("b"), Value: []byte("2")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.AddBlock(
		types.HexToHash("b"),
		types.HexToHash("a"),
		2,
		big.NewInt(2),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("/b/Data"), Value: []byte("Something 1")},
			storage.KeyValue{Key: []byte("/b/Data2"), Value: []byte("Something Else 1")},
			storage.KeyValue{Key: []byte("a"), Value: []byte("3")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.AddBlock(
		types.HexToHash("c"),
		types.HexToHash("a"),
		2,
		big.NewInt(2),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("/c/Data"), Value: []byte("Something 2")},
			storage.KeyValue{Key: []byte("/c/Data2"), Value: []byte("Something Else 2")},
			storage.KeyValue{Key: []byte("b"), Value: []byte("3")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}

	if err := s.View(types.HexToHash("a"), func(tr storage.Transaction) error {
		if data, err := tr.Get([]byte("a")); err != nil {
			return err
		} else if string(data) != "1" {
			t.Errorf("Unexpected value for a")
		}
		if data, err := tr.Get([]byte("b")); err != nil {
			return err
		} else if string(data) != "2" {
			t.Errorf("Unexpected value for b")
		}
		return nil
	}); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.View(types.HexToHash("b"), func(tr storage.Transaction) error {
		if data, err := tr.Get([]byte("a")); err != nil {
			return err
		} else if string(data) != "3" {
			t.Errorf("Unexpected value for a")
		}
		if data, err := tr.Get([]byte("b")); err != nil {
			return err
		} else if string(data) != "2" {
			t.Errorf("Unexpected value for b")
		}
		if data, err := tr.Get([]byte("/a/Data")); err != nil {
			return err
		} else if string(data) != "Something" {
			t.Errorf("Unexpected value for a.Data")
		}
		if data, err := tr.Get([]byte("/b/Data")); err != nil {
			return err
		} else if string(data) != "Something 1" {
			t.Errorf("Unexpected value for a.Data")
		}
		return nil
	}); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.View(types.HexToHash("c"), func(tr storage.Transaction) error {
		if data, err := tr.Get([]byte("a")); err != nil {
			return err
		} else if string(data) != "1" {
			t.Errorf("Unexpected value for a")
		}
		if data, err := tr.Get([]byte("b")); err != nil {
			return err
		} else if string(data) != "3" {
			t.Errorf("Unexpected value for b")
		}
		return nil
	}); err != nil {
		t.Errorf(err.Error())
	}
}
func TestDepth(t *testing.T) {
	db := mem.NewMemoryDatabase(1024)
	s := New(db, 4, nil)
	if err := s.AddBlock(
		types.HexToHash("a"),
		types.Hash{},
		1,
		big.NewInt(1),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("/a/Data"), Value: []byte("Something")},
			storage.KeyValue{Key: []byte("/a/Data2"), Value: []byte("Something Else")},
			storage.KeyValue{Key: []byte("a"), Value: []byte("1")},
			storage.KeyValue{Key: []byte("b"), Value: []byte("2")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.AddBlock(
		types.HexToHash("b"),
		types.HexToHash("a"),
		2,
		big.NewInt(2),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("/a/Data"), Value: []byte("Something 1")},
			storage.KeyValue{Key: []byte("/a/Data2"), Value: []byte("Something Else 1")},
			storage.KeyValue{Key: []byte("a"), Value: []byte("3")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.AddBlock(
		types.HexToHash("c"),
		types.HexToHash("a"),
		2,
		big.NewInt(2),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("/a/Data"), Value: []byte("Something 2")},
			storage.KeyValue{Key: []byte("/a/Data2"), Value: []byte("Something Else 2")},
			storage.KeyValue{Key: []byte("b"), Value: []byte("3")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.AddBlock(
		types.HexToHash("d"),
		types.HexToHash("c"),
		3,
		big.NewInt(3),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("/a/Data"), Value: []byte("Something 2")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.AddBlock(
		types.HexToHash("e"),
		types.HexToHash("d"),
		4,
		big.NewInt(4),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("Data"), Value: []byte("Something 2")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.AddBlock(
		types.HexToHash("f"),
		types.HexToHash("e"),
		5,
		big.NewInt(5),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("Data"), Value: []byte("Something 2")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.AddBlock(
		types.HexToHash("g"),
		types.HexToHash("f"),
		5,
		big.NewInt(5),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("Data"), Value: []byte("Something 2")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}

	if err := s.View(types.HexToHash("a"), func(tr storage.Transaction) error { return nil }); err != storage.ErrLayerNotFound {
		t.Errorf("Expected missing layer, got: %v", err)
	}
	if err := s.View(types.HexToHash("b"), func(tr storage.Transaction) error { return nil }); err != storage.ErrLayerNotFound {
		t.Errorf("Expected missing layer, got: %v", err)
	}
	if err := s.View(types.HexToHash("g"), func(tr storage.Transaction) error {
		if data, err := tr.Get([]byte("a")); err != nil {
			t.Errorf("Error getting 'a': %v", err)
			return err
		} else if string(data) != "1" {
			t.Errorf("Unexpected value for a")
		}
		if data, err := tr.Get([]byte("b")); err != nil {
			t.Errorf("Error getting 'b': %v", err)
			return err
		} else if string(data) != "3" {
			t.Errorf("Unexpected value for b")
		}
		return nil
	}); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.Rollback(1); err != nil {
		t.Fatalf(err.Error())
	}
	if err := s.View(types.HexToHash("a"), func(tr storage.Transaction) error {
		if data, err := tr.Get([]byte("b")); err != nil {
			return err
		} else if string(data) != "2" {
			t.Errorf("Unexpected value for a: %v", string(data))
		}
		return nil
	}); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestCloseAndReopen(t *testing.T) {
	db := mem.NewMemoryDatabase(1024)
	s := New(db, 4, nil)
	s.RegisterWaiter(newTestWaiter(t), 250*time.Millisecond)
	if err := s.AddBlock(
		types.HexToHash("a"),
		types.Hash{},
		1,
		big.NewInt(1),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("/a/Data"), Value: []byte("Something")},
			storage.KeyValue{Key: []byte("/a/Data2"), Value: []byte("Something Else")},
			storage.KeyValue{Key: []byte("a"), Value: []byte("1")},
			storage.KeyValue{Key: []byte("b"), Value: []byte("2")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.AddBlock(
		types.HexToHash("c"),
		types.HexToHash("a"),
		2,
		big.NewInt(2),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("/a/Data"), Value: []byte("Something 2")},
			storage.KeyValue{Key: []byte("/a/Data2"), Value: []byte("Something Else 2")},
			storage.KeyValue{Key: []byte("b"), Value: []byte("3")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.AddBlock(
		types.HexToHash("d"),
		types.HexToHash("c"),
		3,
		big.NewInt(3),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("/a/Data"), Value: []byte("Something 2")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.AddBlock(
		types.HexToHash("e"),
		types.HexToHash("d"),
		4,
		big.NewInt(4),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("Data"), Value: []byte("Something 2")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.AddBlock(
		types.HexToHash("f"),
		types.HexToHash("e"),
		5,
		big.NewInt(5),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("Data"), Value: []byte("Something 2")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.AddBlock(
		types.HexToHash("g"),
		types.HexToHash("f"),
		5,
		big.NewInt(5),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("Data"), Value: []byte("Something 2")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}
	if err := s.Close(); err != nil {
		t.Fatalf(err.Error())
	}
	var err error
	s, err = Open(db, 4, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if err := s.View(types.HexToHash("g"), func(tr storage.Transaction) error {
		if data, err := tr.Get([]byte("a")); err != nil {
			t.Errorf("Error getting 'a': %v", err)
			return err
		} else if string(data) != "1" {
			t.Errorf("Unexpected value for a")
		}
		if data, err := tr.Get([]byte("b")); err != nil {
			t.Errorf("Error getting 'b': %v", err)
			return err
		} else if string(data) != "3" {
			t.Errorf("Unexpected value for b")
		}
		return nil
	}); err != nil {
		t.Errorf(err.Error())
	}
}


func TestWaiterBlock(t *testing.T) {
	db := mem.NewMemoryDatabase(1024)
	s := New(db, 8, nil)
	tw := newTestWaiter(t)
	s.RegisterWaiter(tw, 250*time.Millisecond)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if h, _ := s.NumberToHash(1); h != types.HexToHash("a") {
			t.Errorf("unexpected hash")
		}
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		s.View(types.HexToHash("a"), func(tr storage.Transaction) error {
			data, err := tr.Get([]byte("a"))
			if err != nil {
				t.Errorf(err.Error())
			}
			if string(data) != "1" {
				t.Errorf("Unexpected data value")
			}
			return nil
		})
		wg.Done()
	}()
	if err := s.AddBlock(
		types.HexToHash("a"),
		types.Hash{},
		1,
		big.NewInt(1),
		[]storage.KeyValue{
			storage.KeyValue{Key: []byte("a"), Value: []byte("1")},
		},
		[][]byte{},
		[]byte("0"),
	); err != nil {
		t.Errorf(err.Error())
	}
	tw.release(types.HexToHash("a"), 1)
	wg.Wait()
}