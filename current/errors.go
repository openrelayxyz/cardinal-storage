package current

import (
  "errors"
)

var (
  ErrNotFound = errors.New("Not Found")
  ErrLayerNotFound = errors.New("Layer Not Found")
)
