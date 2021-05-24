package current

import (
  // "errors"
  // "bytes"
  "testing"
  "math/big"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/openrelayxyz/cardinal-storage"
  "github.com/openrelayxyz/cardinal-storage/db/mem"
)


func TestAddBlock(t *testing.T) {
  db := mem.NewMemoryDatabase(1024)
  s := New(db, 8)
  if err := s.AddBlock(
    types.HexToHash("a"),
    types.Hash{},
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("Data"), Value: []byte("Something")},
      storage.KeyValue{Key: []byte("Data2"), Value: []byte("Something Else")},
    },
    1,
    big.NewInt(1),
    [][]byte{},
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("a"), Value: []byte("1")},
      storage.KeyValue{Key: []byte("b"), Value: []byte("2")},
    },
    []byte("0"),
  );  err != nil { t.Errorf(err.Error()) }
  if err := s.AddBlock(
    types.HexToHash("b"),
    types.HexToHash("a"),
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("Data"), Value: []byte("Something 1")},
      storage.KeyValue{Key: []byte("Data2"), Value: []byte("Something Else 1")},
    },
    2,
    big.NewInt(2),
    [][]byte{},
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("a"), Value: []byte("3")},
    },
    []byte("0"),
  );  err != nil { t.Errorf(err.Error()) }

  if err := s.View(types.HexToHash("a"), func(tr storage.Transaction) error {
    if data, err := tr.GetState([]byte("a")); err != nil {
      return err
    } else if string(data) != "1" { t.Errorf("Unexpected value for a")}
    if data, err := tr.GetState([]byte("b")); err != nil {
      return err
    } else if string(data) != "2" { t.Errorf("Unexpected value for b")}
    return nil
  }); err != nil { t.Errorf(err.Error() )}
  if err := s.View(types.HexToHash("b"), func(tr storage.Transaction) error {
    if data, err := tr.GetState([]byte("a")); err != nil {
      return err
    } else if string(data) != "3" { t.Errorf("Unexpected value for a")}
    if data, err := tr.GetState([]byte("b")); err != nil {
      return err
    } else if string(data) != "2" { t.Errorf("Unexpected value for b")}
    if data, err := tr.GetBlockData(types.HexToHash("a"), []byte("Data")); err != nil {
      return err
    } else if string(data) != "Something" { t.Errorf("Unexpected value for a.Data")}
    if data, err := tr.GetBlockData(types.HexToHash("b"), []byte("Data")); err != nil {
      return err
    } else if string(data) != "Something 1" { t.Errorf("Unexpected value for a.Data")}
    return nil
  }); err != nil { t.Errorf(err.Error() )}
}
func TestFork(t *testing.T) {
  db := mem.NewMemoryDatabase(1024)
  s := New(db, 8)
  if err := s.AddBlock(
    types.HexToHash("a"),
    types.Hash{},
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("Data"), Value: []byte("Something")},
      storage.KeyValue{Key: []byte("Data2"), Value: []byte("Something Else")},
    },
    1,
    big.NewInt(1),
    [][]byte{},
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("a"), Value: []byte("1")},
      storage.KeyValue{Key: []byte("b"), Value: []byte("2")},
    },
    []byte("0"),
  );  err != nil { t.Errorf(err.Error()) }
  if err := s.AddBlock(
    types.HexToHash("b"),
    types.HexToHash("a"),
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("Data"), Value: []byte("Something 1")},
      storage.KeyValue{Key: []byte("Data2"), Value: []byte("Something Else 1")},
    },
    2,
    big.NewInt(2),
    [][]byte{},
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("a"), Value: []byte("3")},
    },
    []byte("0"),
  );  err != nil { t.Errorf(err.Error()) }
  if err := s.AddBlock(
    types.HexToHash("c"),
    types.HexToHash("a"),
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("Data"), Value: []byte("Something 2")},
      storage.KeyValue{Key: []byte("Data2"), Value: []byte("Something Else 2")},
    },
    2,
    big.NewInt(2),
    [][]byte{},
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("b"), Value: []byte("3")},
    },
    []byte("0"),
  );  err != nil { t.Errorf(err.Error()) }

  if err := s.View(types.HexToHash("a"), func(tr storage.Transaction) error {
    if data, err := tr.GetState([]byte("a")); err != nil {
      return err
    } else if string(data) != "1" { t.Errorf("Unexpected value for a")}
    if data, err := tr.GetState([]byte("b")); err != nil {
      return err
    } else if string(data) != "2" { t.Errorf("Unexpected value for b")}
    return nil
  }); err != nil { t.Errorf(err.Error() )}
  if err := s.View(types.HexToHash("b"), func(tr storage.Transaction) error {
    if data, err := tr.GetState([]byte("a")); err != nil {
      return err
    } else if string(data) != "3" { t.Errorf("Unexpected value for a")}
    if data, err := tr.GetState([]byte("b")); err != nil {
      return err
    } else if string(data) != "2" { t.Errorf("Unexpected value for b")}
    if data, err := tr.GetBlockData(types.HexToHash("a"), []byte("Data")); err != nil {
      return err
    } else if string(data) != "Something" { t.Errorf("Unexpected value for a.Data")}
    if data, err := tr.GetBlockData(types.HexToHash("b"), []byte("Data")); err != nil {
      return err
    } else if string(data) != "Something 1" { t.Errorf("Unexpected value for a.Data")}
    return nil
  }); err != nil { t.Errorf(err.Error() )}
  if err := s.View(types.HexToHash("c"), func(tr storage.Transaction) error {
    if data, err := tr.GetState([]byte("a")); err != nil {
      return err
    } else if string(data) != "1" { t.Errorf("Unexpected value for a")}
    if data, err := tr.GetState([]byte("b")); err != nil {
      return err
    } else if string(data) != "3" { t.Errorf("Unexpected value for b")}
    return nil
  }); err != nil { t.Errorf(err.Error() )}
}
func TestDepth(t *testing.T) {
  db := mem.NewMemoryDatabase(1024)
  s := New(db, 4)
  if err := s.AddBlock(
    types.HexToHash("a"),
    types.Hash{},
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("Data"), Value: []byte("Something")},
      storage.KeyValue{Key: []byte("Data2"), Value: []byte("Something Else")},
    },
    1,
    big.NewInt(1),
    [][]byte{},
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("a"), Value: []byte("1")},
      storage.KeyValue{Key: []byte("b"), Value: []byte("2")},
    },
    []byte("0"),
  );  err != nil { t.Errorf(err.Error()) }
  if err := s.AddBlock(
    types.HexToHash("b"),
    types.HexToHash("a"),
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("Data"), Value: []byte("Something 1")},
      storage.KeyValue{Key: []byte("Data2"), Value: []byte("Something Else 1")},
    },
    2,
    big.NewInt(2),
    [][]byte{},
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("a"), Value: []byte("3")},
    },
    []byte("0"),
  );  err != nil { t.Errorf(err.Error()) }
  if err := s.AddBlock(
    types.HexToHash("c"),
    types.HexToHash("a"),
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("Data"), Value: []byte("Something 2")},
      storage.KeyValue{Key: []byte("Data2"), Value: []byte("Something Else 2")},
    },
    2,
    big.NewInt(2),
    [][]byte{},
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("b"), Value: []byte("3")},
    },
    []byte("0"),
  );  err != nil { t.Errorf(err.Error()) }
  if err := s.AddBlock(
    types.HexToHash("d"),
    types.HexToHash("c"),
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("Data"), Value: []byte("Something 2")},
    },
    3,
    big.NewInt(3),
    [][]byte{},
    []storage.KeyValue{},
    []byte("0"),
  );  err != nil { t.Errorf(err.Error()) }
  if err := s.AddBlock(
    types.HexToHash("e"),
    types.HexToHash("d"),
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("Data"), Value: []byte("Something 2")},
    },
    4,
    big.NewInt(4),
    [][]byte{},
    []storage.KeyValue{},
    []byte("0"),
  );  err != nil { t.Errorf(err.Error()) }
  if err := s.AddBlock(
    types.HexToHash("f"),
    types.HexToHash("e"),
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("Data"), Value: []byte("Something 2")},
    },
    5,
    big.NewInt(5),
    [][]byte{},
    []storage.KeyValue{},
    []byte("0"),
  );  err != nil { t.Errorf(err.Error()) }
  if err := s.AddBlock(
    types.HexToHash("g"),
    types.HexToHash("f"),
    []storage.KeyValue{
      storage.KeyValue{Key: []byte("Data"), Value: []byte("Something 2")},
    },
    5,
    big.NewInt(5),
    [][]byte{},
    []storage.KeyValue{},
    []byte("0"),
  );  err != nil { t.Errorf(err.Error()) }

  if err := s.View(types.HexToHash("a"), func(tr storage.Transaction) error { return nil }); err != ErrLayerNotFound { t.Errorf( "Expected missing layer, got: %v", err )}
  if err := s.View(types.HexToHash("b"), func(tr storage.Transaction) error { return nil }); err != ErrLayerNotFound { t.Errorf( "Expected missing layer, got: %v", err )}
  if err := s.View(types.HexToHash("g"), func(tr storage.Transaction) error {
    if data, err := tr.GetState([]byte("a")); err != nil {
      t.Errorf("Error getting 'a': %v", err)
      return err
    } else if string(data) != "1" { t.Errorf("Unexpected value for a")}
    if data, err := tr.GetState([]byte("b")); err != nil {
      t.Errorf("Error getting 'b': %v", err)
      return err
    } else if string(data) != "3" { t.Errorf("Unexpected value for b")}
    return nil
  }); err != nil { t.Errorf(err.Error() )}
}
