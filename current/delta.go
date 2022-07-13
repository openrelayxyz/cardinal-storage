package current

import (
  "bytes"
  "fmt"
  "github.com/hamba/avro"
  "github.com/openrelayxyz/cardinal-storage"
  "github.com/openrelayxyz/cardinal-storage/db"
  log "github.com/inconshreveable/log15"
)

var (
  deltaSchema = avro.MustParse(`{
    "type": "record",
    "name": "delta",
    "avro.codec": "snappy",
    "namespace": "cloud.rivet.cardinal.storage",
    "fields": [
      {
        "name": "changes",
        "type": {"name": "change", "type": "map", "values": {
          "type": "record",
          "name": "deltaEntry",
          "fields": [
            {"name": "data", "type": "bytes"},
            {"name": "delete", "type": "boolean"}
          ]
        }}
      }
    ]
  }`)
)


type deltaEntry struct{
  Data   []byte `avro:"data"`
  Delete bool   `avro:"delete"`
}

type delta struct{
  number  uint64
  tr      db.Transaction
  bw      db.BatchWriter
  Changes map[string]deltaEntry `avro:"changes"`
}

func newDelta(tr db.Transaction, bw db.BatchWriter) *delta {
  return &delta{
    tr: tr,
    bw: bw,
    Changes: make(map[string]deltaEntry),
  }
}

func loadDelta(block uint64, tr db.Transaction, bw db.BatchWriter) (*delta, error) {
  d := newDelta(tr, bw)
  d.number = block
  data, err := d.tr.Get(RollbackDelta(block))
  if err != nil { return nil, err }
  err = avro.Unmarshal(deltaSchema, data, d)
  return d, err
}

func (d *delta) put(key, value []byte) error {
  if _, ok := d.Changes[string(key)]; !ok {
    v, err := d.tr.Get(key)
    if err == storage.ErrNotFound {
      d.Changes[string(key)] = deltaEntry{Delete: true}
    } else if err == nil {
      if !bytes.Equal(v, value) {
        d.Changes[string(key)] = deltaEntry{Data: v}
      }
      // We don't need to track an upsert if the value didn't actually change
    } else {
      log.Error("Database error tracking delta", "key", fmt.Sprintf("%x", key), "error", err)
    }
  }
  return d.bw.Put(key, value)
}

func (d *delta) delete(key []byte) error {
  if _, ok := d.Changes[string(key)]; !ok {
    v, err := d.tr.Get(key)
    if err == storage.ErrNotFound {
      d.Changes[string(key)] = deltaEntry{Delete: true}
    } else if err == nil {
      d.Changes[string(key)] = deltaEntry{Data: v}
    } else {
      log.Error("Database error tracking delta delete", "key", fmt.Sprintf("%v", key), "error", err)
    }
  }
  return d.bw.Delete(key)
}

func (d *delta) finalize(blockNumber uint64) error {
  data, err := avro.Marshal(deltaSchema, d)
  if err != nil { return err }
  if err := d.bw.Put(RollbackDelta(blockNumber), data); err != nil {
    return err
  }
  return d.bw.Flush()
}

func (d *delta) apply() error {
  for k, v := range d.Changes {
    if v.Delete {
      log.Debug("Rollback deletion", "key", fmt.Sprintf("%#x", k))
      if err := d.bw.Delete([]byte(k)); err != nil && err != storage.ErrNotFound { return err }
    } else {
      log.Debug("Rollback update", "key", fmt.Sprintf("%#x", k), "value", fmt.Sprintf("%#x", v.Data))
      if err := d.bw.Put([]byte(k), v.Data); err != nil { return err }
    }
  }
  log.Debug("Rolled back", "block", d.number)
  if err := d.bw.Delete(RollbackDelta(d.number)); err != nil { return err }
	return d.bw.Flush()
}


// TODO: Think about cleaning up old deltas
