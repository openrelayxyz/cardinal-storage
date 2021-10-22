package storage

import (
  "errors"
)

var (
  ErrNotFound = errors.New("Not Found")
  ErrLayerNotFound = errors.New("Layer Not Found")
	ErrWriteToReadOnly = errors.New("attempted write to read-only transaction")
)
