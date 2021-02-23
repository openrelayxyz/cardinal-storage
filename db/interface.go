package db

type Database interface {
  View(func(Transaction) error)
  Update(func(Transaction) error)
}

type Transaction interface {
  Get([]byte) ([]byte, error)
  Put([]byte, []byte) error
  PutReserve([]byte, int) ([]byte, error)
  Delete([]byte) error
}
