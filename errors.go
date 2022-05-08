package storage

import (
  "errors"
)

var (
  ErrNotFound = errors.New("Not Found")
  ErrLayerNotFound = errors.New("Block Not Found")
	ErrWriteToReadOnly = errors.New("attempted write to read-only transaction")
	ErrUnknownStorageType = errors.New("unknown storage type")
)
