package main

import (
  dbpkg "github.com/openrelayxyz/cardinal-storage/db"
  "github.com/openrelayxyz/cardinal-storage/db/badgerdb"
  "github.com/openrelayxyz/cardinal-storage/current"
  log "github.com/inconshreveable/log15"
  "os"
)


func main() {
	var db dbpkg.Database
	var err error
	if len(os.Args) == 2 {
		db, err = badgerdb.NewReadOnly(os.Args[1])
	} else {
		db, err = badgerdb.New(os.Args[1])
	}
  if err != nil {
    log.Error("Error opening badgerdb", "error", err)
    os.Exit(1)
  }
  defer db.Close()
  if err := db.Update(func(tx dbpkg.Transaction) error {
    if len(os.Args) == 2 {
      // Get
      resumption, err := tx.Get(current.ResumptionDataKey)
      if err != nil {
        log.Error("Error getting Resumption token", "error", err)
        os.Exit(1)
      }
      log.Info("Resumption token", "token", string(resumption))
    } else if len(os.Args) == 3 {
      // Put
      if err := tx.Put(current.ResumptionDataKey, []byte(os.Args[2])); err != nil {
        log.Error("Error setting resumption token", "error", err)
        os.Exit(1)
      }
      log.Info("Set resumption token")
    } else {
      log.Error("Invalid arguments", "args", os.Args)
      os.Exit(1)
    }
    return nil
  }); err != nil {
    log.Error("Error starting transaction", "error", err)
    os.Exit(1)
  }
}
